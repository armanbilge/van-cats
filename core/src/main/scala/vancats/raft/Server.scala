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

import cats.Monad
import cats.effect.Temporal
import cats.effect.kernel.{Concurrent, Deferred}
import cats.effect.std.Random
import cats.effect.syntax.all._
import cats.syntax.all._
import com.google.protobuf.ByteString
import fs2.concurrent.Channel
import fs2.{Pipe, Stream}
import io.grpc.Metadata
import vancats.raft.Role.{Candidate, Follower, Leader}
import vancats.raft.Server.Command

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

final case class State private[raft] (
    role: Role = Follower(),
    currentTerm: Int = 0,
    votedFor: Option[Int] = None,
    log: Seq[LogEntry] = Seq.empty,
    commitIndex: Int = 0, // All entries strictly below this index have been committed
    lastApplied: Int = 0 // All entries strictly below this index have been applied
) {

  private[raft] def becomeFollower[F[_]: Monad](
      leaderId: Option[Int],
      scheduleTimeout: Int => F[Unit]): F[State] = {
    val timeoutId = role.timeoutId.fold(0)(_ + 1)
    scheduleTimeout(timeoutId).as(copy(role = Follower(leaderId, timeoutId), votedFor = None))
  }

  private[raft] def updateTerm[F[_]: Monad](
      command: Command[F],
      scheduleTimeout: Int => F[Unit]): F[State] =
    command.term match {
      case Some(term) if term > currentTerm =>
        copy(currentTerm = term).becomeFollower(command.leaderId, scheduleTimeout)
      case _ => (this: State).pure
    }

  private[raft] def tryIncrementCommitIndex(serverCount: Int): State =
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

  private[raft] def commit[F[_]: Concurrent](commit: Pipe[F, ByteString, Nothing]): F[State] =
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
      (this: State).pure

}

sealed abstract class Role {
  def isFollower = false
  def isLeader = false
  private[raft] def timeoutId: Option[Int] = None
}
private[raft] object Role {

  final case class Follower(leader: Option[Int] = None, _timeoutId: Int = 0) extends Role {
    override def isFollower = true
    override def timeoutId = Some(_timeoutId)
  }

  final case class Candidate(votes: Int = 1, _timeoutId: Int) extends Role {
    override def timeoutId = Some(_timeoutId)
  }

  final case class Leader(
      nextIndex: Map[Int, Int], // Index of next entry to send
      matchIndex: Map[
        Int,
        Int
      ] // All entries strictly below this index are known to be replicated
  ) extends Role {
    override def isLeader = true
  }
}

sealed abstract private[raft] class Server[F[_]] {
  def command: Pipe[F, Command[F], Nothing]
  def command1(command: Command[F]): F[Unit]
  def state: Stream[F, State]
}

private[raft] object Server {

  sealed abstract class Command[F[_]] {
    def term: Option[Int] = None
    def leaderId: Option[Int] = None
  }

  final case class ServiceClient[F[_]](command: ByteString) extends Command[F]

  final case class RequestVote[F[_]](
      request: RequestVoteRequest,
      reply: Deferred[F, RequestVoteReply])
      extends Command[F] {
    override def term = Some(request.term)
  }

  final case class CastBallot[F[_]](reply: RequestVoteReply) extends Command[F] {
    override def term = Some(reply.term)
  }

  final case class AppendEntries[F[_]](
      request: AppendEntriesRequest,
      reply: Deferred[F, AppendEntriesReply])
      extends Command[F] {
    override def term = Some(request.term)
    override def leaderId = Some(request.leaderId)
  }

  final case class ProcessAppendEntriesReply[F[_]](
      peerId: Int,
      targetNextIndex: Int,
      reply: AppendEntriesReply)
      extends Command[F]

  final case class BeatHeart[F[_]](forTerm: Int) extends Command[F]
  final case class StartElection[F[_]](id: Int) extends Command[F]

  def apply[F[_]: Temporal: Random](
      id: Int,
      serverCount: Int,
      heartRate: FiniteDuration,
      electionTimeout: FiniteDuration,
      electionTimeoutNoiseFactor: Double,
      peers: Map[Int, RaftFs2Grpc[F, Metadata]],
      commit: Pipe[F, ByteString, Nothing]
  ): F[Server[F]] = {

    for {

      channel <- Channel.unbounded[F, Command[F]]
      scheduleElectionTimeout = (id: Int) =>
        for {
          noise <- Random[F].betweenDouble(0, electionTimeoutNoiseFactor)
          timeout = electionTimeout * (1 + noise) match {
            case fd: FiniteDuration => fd
            case _ => electionTimeout
          }
          _ <- channel.send(StartElection(id)).delayBy(timeout).start
        } yield ()
      scheduleHeartBeat = (term: Int) =>
        channel.send(BeatHeart(term)).delayBy(heartRate).start.void
      _ <- scheduleElectionTimeout(0)
      fsm = channel
        .stream
        .evalScan(State()) { (server, command) =>
          server.updateTerm(command, scheduleElectionTimeout).flatMap {
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
                          follower <- state
                            .becomeFollower(Some(leaderId), scheduleElectionTimeout)
                          committed <- follower
                            .copy(commitIndex =
                              if (leaderCommit > commitIndex) leaderCommit min updatedLog.size
                              else commitIndex)
                            .commit(commit)
                        } yield committed
                      } else if (term >= currentTerm)
                        state.becomeFollower(Some(leaderId), scheduleElectionTimeout)
                      else state.pure
                  } yield server

                case ProcessAppendEntriesReply(
                      peerId,
                      targetNextIndex,
                      AppendEntriesReply(_, success)) =>
                  role match {
                    case Leader(nextIndex, matchIndex) if success =>
                      state
                        .copy(role = Leader(
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

                case StartElection(timeoutId) =>
                  role.timeoutId match {
                    case Some(expectedId) if timeoutId == expectedId =>
                      scheduleElectionTimeout(id + 1) *>
                        peers.values.toList.traverse { peer =>
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
                        } as state.copy(
                          role = Candidate(_timeoutId = timeoutId + 1),
                          currentTerm = currentTerm + 1,
                          votedFor = Some(id)
                        )
                    case _ => state.pure
                  }

              }
          }
        }

    } yield new Server[F] {
      override def command: Pipe[F, Command[F], Nothing] =
        _.evalMap(command1).drain

      override def command1(command: Command[F]): F[Unit] =
        channel.send(command).void

      override val state: Stream[F, State] = fsm
    }
  }

}
