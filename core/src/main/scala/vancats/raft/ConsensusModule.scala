/*
 * Copyright 2021 Arman Bilge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vancats.raft

import cats.effect.Temporal
import cats.effect.kernel.{Concurrent, Deferred}
import cats.effect.std.Random
import cats.effect.syntax.all._
import cats.syntax.all._
import com.google.protobuf.ByteString
import fs2.concurrent.Channel
import fs2.{Pipe, Stream}
import io.grpc.Metadata
import vancats.raft.ConsensusModule.{Command, State}

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

sealed abstract private[raft] class ConsensusModule[F[_]] {
  def command: Pipe[F, Command[F], Nothing]
  def command1(command: Command[F]): F[Unit]
  def state: Stream[F, State[F]]
}

private object ConsensusModule {

  import Role.{Candidate, Follower, Leader}

  final case class State[F[_]] private[ConsensusModule] (
      role: Role[F],
      currentTerm: Int = 0,
      votedFor: Option[Int] = None,
      log: Seq[LogEntry] = Seq.empty,
      commitIndex: Int = 0, // All entries strictly below this index have been committed
      lastApplied: Int = 0 // All entries strictly below this index have been applied
  ) {

    private[ConsensusModule] def becomeFollower(
        leaderId: Option[Int],
        scheduleElection: Int => F[F[Unit]])(implicit F: Concurrent[F]): F[State[F]] = for {
      id <- role.nextElectionOption.fold(0.pure) {
        case NextElection(id, cancel) =>
          cancel.start.as(id + 1)
      }
      cancel <- scheduleElection(id)
    } yield copy(role = Follower(leaderId, NextElection(id, cancel)))

    private[ConsensusModule] def updateTerm(
        command: Command[F],
        scheduleElection: Int => F[F[Unit]])(implicit F: Concurrent[F]): F[State[F]] =
      command.term match {
        case Some(term) if term > currentTerm =>
          copy(currentTerm = term, votedFor = None)
            .becomeFollower(command.leaderId, scheduleElection)
        case _ => (this: State[F]).pure
      }

    private[ConsensusModule] def tryIncrementCommitIndex(serverCount: Int): State[F] =
      role match {
        case Leader(_, matchIndex) =>
          @tailrec def loop(N: Int): Option[Int] =
            if (N > commitIndex) {
              if (log(N - 1).term == currentTerm && matchIndex
                  .values
                  .count(_ >= N) > serverCount / 2)
                Some(N)
              else
                loop(N - 1)
            } else None

          loop(log.size).fold(this)(i => copy(commitIndex = i))
        case _ => this
      }

    private[ConsensusModule] def commit(commit: Pipe[F, ByteString, Nothing])(
        implicit F: Concurrent[F]): F[State[F]] =
      if (commitIndex > lastApplied)
        Stream
          .emits(log.slice(lastApplied, commitIndex))
          .map(_.command)
          .through(commit)
          .compile
          .drain
          .start
          .as(copy(lastApplied = commitIndex min log.size))
      else
        (this: State[F]).pure

  }

  sealed abstract class Role[F[_]] {
    def isFollower = false
    def isLeader = false
    private[ConsensusModule] def nextElectionOption: Option[NextElection[F]] = None
  }
  private[ConsensusModule] object Role {

    final case class Follower[F[_]](leader: Option[Int] = None, nextElection: NextElection[F])
        extends Role[F] {
      override def isFollower = true
      override def nextElectionOption = Some(nextElection)
    }

    final case class Candidate[F[_]](votes: Int = 1, nextElection: NextElection[F])
        extends Role[F] {
      override def nextElectionOption = Some(nextElection)
    }

    final case class Leader[F[_]](
        nextIndex: Map[Int, Int], // Index of next entry to send
        matchIndex: Map[
          Int,
          Int
        ] // All entries strictly below this index are known to be replicated
    ) extends Role[F] {
      override def isLeader = true
    }
  }

  final private case class NextElection[F[_]](id: Int, cancel: F[Unit])

  sealed abstract private[raft] class Command[F[_]] {
    def term: Option[Int] = None
    def leaderId: Option[Int] = None
  }

  final private[raft] case class ServiceClient[F[_]](command: ByteString) extends Command[F]

  final private[raft] case class RequestVote[F[_]](
      request: RequestVoteRequest,
      reply: Deferred[F, RequestVoteReply])
      extends Command[F] {
    override def term = Some(request.term)
  }

  final private[raft] case class CastBallot[F[_]](reply: RequestVoteReply) extends Command[F] {
    override def term = Some(reply.term)
  }

  final private[raft] case class AppendEntries[F[_]](
      request: AppendEntriesRequest,
      reply: Deferred[F, AppendEntriesReply])
      extends Command[F] {
    override def term = Some(request.term)
    override def leaderId = Some(request.leaderId)
  }

  final private[raft] case class ProcessAppendEntriesReply[F[_]](
      peerId: Int,
      targetNextIndex: Int,
      reply: AppendEntriesReply)
      extends Command[F]

  final private case class BeatHeart[F[_]](forTerm: Int) extends Command[F]
  final private case class StartElection[F[_]](id: Int) extends Command[F]

  private[raft] def apply[F[_]: Temporal: Random](
      id: Int,
      serverCount: Int,
      heartRate: FiniteDuration,
      electionTimeout: FiniteDuration,
      electionTimeoutNoiseFactor: Double,
      peers: Map[Int, RaftFs2Grpc[F, Metadata]],
      commit: Pipe[F, ByteString, Nothing]
  ): F[ConsensusModule[F]] = for {

    channel <- Channel.unbounded[F, Command[F]]
    scheduleElection = (id: Int) =>
      for {
        noise <- Random[F].betweenDouble(0, electionTimeoutNoiseFactor)
        timeout = electionTimeout * (1 + noise) match {
          case fd: FiniteDuration => fd
          case _ => electionTimeout
        }
        fiber <- channel.send(StartElection(id)).delayBy(timeout).start
      } yield fiber.cancel
    scheduleHeartBeat = (term: Int) =>
      channel.send(BeatHeart(term)).delayBy(heartRate).start.void
    cancelElection <- scheduleElection(0)
    fsm = channel
      .stream
      .evalScan(State(Follower(nextElection = NextElection(0, cancelElection)))) {
        (server, command) =>
          server.updateTerm(command, scheduleElection).flatMap {
            case state @ State(role, currentTerm, votedFor, log, commitIndex, _) =>
              def sendAppendEntries(peerId: Int, peer: RaftFs2Grpc[F, Metadata], nextIndex: Int)
                  : F[Unit] =
                peer
                  .appendEntries(
                    AppendEntriesRequest(
                      currentTerm,
                      id,
                      nextIndex,
                      log.lastOption.fold(currentTerm)(_.term),
                      log.drop(nextIndex),
                      commitIndex
                    ),
                    new Metadata)
                  .flatMap(reply =>
                    channel.send(ProcessAppendEntriesReply(peerId, log.size, reply)))
                  .start
                  .void

              command match {
                case ServiceClient(command) =>
                  role match {
                    case Leader(_, _) =>
                      state.copy(log = log :+ LogEntry(currentTerm, command)).pure
                    case Follower(Some(leaderId), _) =>
                      peers(leaderId)
                        .serviceClient(ServiceClientRequest(command), new Metadata)
                        .start
                        .as(state)
                    case _ =>
                      channel.send(ServiceClient(command)).delayBy(heartRate).start.as(state)
                  }

                case RequestVote(
                      RequestVoteRequest(term, candidateId, nextLogIndex, lastLogTerm),
                      reply) =>
                  val grantVote =
                    term >= currentTerm && votedFor.forall(
                      _ == candidateId) && lastLogTerm >= log
                      .lastOption
                      .fold(currentTerm)(_.term) && nextLogIndex >= log.size
                  reply
                    .complete(RequestVoteReply(currentTerm, grantVote))
                    .as(state.copy(votedFor = if (grantVote) Some(candidateId) else votedFor))

                case CastBallot(RequestVoteReply(_, voteGranted)) =>
                  role match {
                    case Candidate(votes, _) if voteGranted && votes + 1 > serverCount / 2 =>
                      scheduleHeartBeat(currentTerm).as(
                        state.copy(
                          role = Leader(
                            Map.empty.withDefaultValue(log.size),
                            Map.empty.withDefaultValue(0)
                          )
                        ))
                    case Candidate(votes, timeoutId) if voteGranted =>
                      state.copy(role = Candidate(votes + 1, timeoutId)).pure
                    case _ =>
                      state.pure
                  }

                case AppendEntries(
                      AppendEntriesRequest(
                        term,
                        leaderId,
                        logIndex,
                        prevLogTerm,
                        entries,
                        leaderCommit),
                      reply) =>
                  val accept = term >= currentTerm &
                    logIndex <= log.size &
                    log.lift(logIndex - 1).forall(_.term == prevLogTerm)
                  for {
                    _ <- reply.complete(AppendEntriesReply(currentTerm, accept))
                    server <-
                      if (accept) {
                        val (unchanged, unverified) = log.splitAt(logIndex)
                        val verified = unverified
                          .lazyZip(entries)
                          .map { (entry, shouldMatch) =>
                            if (entry.term == shouldMatch.term)
                              entry :: Nil
                            else
                              Nil
                          }
                          .takeWhile(_.nonEmpty)
                          .flatten
                        val updatedLog = unchanged ++ verified
                        for {
                          follower <- state.becomeFollower(Some(leaderId), scheduleElection)
                          committed <- follower
                            .copy(commitIndex =
                              if (leaderCommit > commitIndex) leaderCommit min updatedLog.size
                              else commitIndex)
                            .commit(commit)
                        } yield committed
                      } else if (term >= currentTerm)
                        state.becomeFollower(Some(leaderId), scheduleElection)
                      else state.pure
                  } yield server

                case ProcessAppendEntriesReply(
                      peerId,
                      targetNextIndex,
                      AppendEntriesReply(_, success)) =>
                  role match {
                    case Leader(nextIndex, matchIndex) if success =>
                      state
                        .copy[F](role = Leader(
                          nextIndex.updated(peerId, targetNextIndex),
                          matchIndex.updated(peerId, targetNextIndex)))
                        .tryIncrementCommitIndex(serverCount)
                        .commit(commit)
                    case Leader(nextIndex, matchIndex) if !success =>
                      sendAppendEntries(peerId, peers(peerId), nextIndex(peerId) - 1) as
                        state.copy(role =
                          Leader(nextIndex.updatedWith(peerId)(_.map(_ - 1)), matchIndex))
                    case _ => state.pure
                  }

                case BeatHeart(forTerm) =>
                  role match {
                    case Leader(nextIndex, _) if forTerm == currentTerm =>
                      scheduleHeartBeat(currentTerm) *>
                        peers.toList.traverse {
                          case (peerId, peer) =>
                            sendAppendEntries(peerId, peer, nextIndex(peerId))
                        } as state
                    case _ => state.pure
                  }

                case StartElection(electionId) =>
                  role.nextElectionOption match {
                    case Some(NextElection(expectedId, _)) if electionId == expectedId =>
                      for {
                        cancelElection <- scheduleElection(electionId + 1)
                        _ <- peers.values.toList.traverse { peer =>
                          peer
                            .requestVote(
                              RequestVoteRequest(
                                currentTerm + 1,
                                id,
                                log.size,
                                log.lastOption.fold(currentTerm + 1)(_.term)),
                              new Metadata())
                            .flatMap(reply => channel.send(CastBallot(reply)))
                            .start
                        }
                      } yield state.copy(
                        role = Candidate(nextElection =
                          NextElection(electionId + 1, cancelElection)),
                        currentTerm = currentTerm + 1,
                        votedFor = Some(id)
                      )
                    case _ => state.pure
                  }

              }
          }
      }

  } yield new ConsensusModule[F] {
    override def command: Pipe[F, Command[F], Nothing] =
      _.evalMap(command1).drain

    override def command1(command: Command[F]): F[Unit] =
      channel.send(command).void

    override val state: Stream[F, State[F]] = fsm
  }

}
