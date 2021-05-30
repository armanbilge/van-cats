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

import cats.effect.std.Random
import cats.effect.testing.specs2.CatsEffect
import cats.effect.{Deferred, IO}
import cats.syntax.all._
import com.google.protobuf.ByteString
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.specs2.execute.Result
import org.specs2.mutable.Specification
import org.specs2.specification.core.{AsExecution, Execution, Fragments}
import vancats.raft.ConsensusModule.Role.{Follower, Leader}
import vancats.raft.ConsensusModule.State
import vancats.raft.RaftSpec._

import scala.concurrent.duration._

object RaftSpec {
  final case class TestRaft(
      raft: GrpcRaft[IO],
      connected: IO[Boolean],
      connect: IO[Unit],
      disconnect: IO[Unit])

  sealed abstract class Status
  case object Initial extends Status
  case object Target extends Status
  final case class Epoch(epoch: Int) extends Status
  final case class Reign(epoch: Int, leader: Int, term: Int) extends Status

  final case class ClusterState(members: Seq[State[IO]], connected: Seq[Boolean]) {

    def soleLeaderId: Option[Int] =
      Some(members.indices.toList.filter(i => connected(i) & members(i).role.isLeader))
        .collect { case id :: Nil => id }
        .filter { leaderId =>
          members.indices.toList.filter(connected).map(members(_).role).forall {
            case Leader(_, _) => true
            case Follower(Some(`leaderId`), _) => true
            case _ => false
          }
        }

    def hasNoLeader: Boolean = !members.lazyZip(connected).exists(_.role.isLeader & _)

    def isCommitted(command: ByteString): Boolean =
      connected.exists(identity) && members.lazyZip(connected).forall { (state, connected) =>
        !connected || {
          val index = state.log.lastIndexWhere(_.command == command)
          index != -1 && state.commitIndex > index
        }
      }
  }
}

class RaftSpec extends Specification with CatsEffect {

  override val Timeout: FiniteDuration = 10.seconds

  def createCluster(size: Int): Stream[IO, (Seq[TestRaft], Stream[IO, ClusterState])] =
    Stream.eval {
      for {
        deferredServers <- Vector.fill(size)(Deferred[IO, GrpcRaft[IO]]).sequence
        servers = deferredServers.map(GrpcRaft.deferred[IO])
        signal <- SignallingRef[IO, Map[Int, State[IO]]](Map.empty)
        rafts <- deferredServers.traverseWithIndexM { (deferred, id) =>
          val peers = servers.zipWithIndex.map(_.swap).toMap.removed(id)
          Random.scalaUtilRandom[IO].flatMap { implicit random =>
            for {
              connected <- IO.ref(true)
              raft <- GrpcRaft[IO](
                id,
                peers.view.mapValues(GrpcRaft.flaky(_, connected)).toMap,
                GrpcRaft.Config(10.milliseconds, 100.milliseconds, 0.5))
              _ <- deferred.complete(GrpcRaft.flaky(raft, connected))
              stream = raft.state.foreach(s => signal.update(_.updated(id, s))).drain
            } yield (
              TestRaft(raft, connected.get, connected.set(true), connected.set(false)),
              stream)
          }
        }
        testRafts = rafts.map(_._1)
        stream = rafts
          .foldLeft(signal.discrete)((acc, x) => acc.concurrently(x._2))
          .collect {
            case members if members.size == size => Seq.tabulate(size)(members)
          }
          .zipWith(Stream.repeatEval(testRafts.traverse(_.connected)))(ClusterState.apply)
      } yield (testRafts, stream)
    }

  implicit object streamAsExecution extends AsExecution[Stream[IO, Status]] {
    override def execute(t: => Stream[IO, Status]): Execution =
      effectAsExecution[IO, Result].execute {
        t.collectFirst { case Target => () }.compile.lastOrError.as(success).timeout(Timeout)
      }
  }

  "GrpcRaft" should {

    Fragments.foreach(List(3, 5)) { size =>
      implicit class StringOps(s: String) {
        def withSize = s"$s in cluster of $size"
      }

      "elect a single leader".withSize in createCluster(size).flatMap {
        case (_, states) =>
          states.find(_.soleLeaderId.isDefined).as(Target: Status)
      }

      "elect a new leader".withSize in createCluster(size).flatMap {
        case (rafts, states) =>
          states.evalScan(Initial: Status) {
            case (Initial, cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              val leader = cluster.members(leaderId)
              rafts(leaderId).disconnect.as(Reign(0, leaderId, leader.currentTerm))
            case (Reign(0, prevLeader, prevTerm), cluster) if cluster.soleLeaderId.exists {
                  id => id != prevLeader && cluster.members(id).currentTerm > prevTerm
                } =>
              IO.pure(Target)
            case (status, _) => IO.pure(status)
          }
      }

      "elect a new leader once a quorum is reached".withSize in createCluster(size).flatMap {
        case (rafts, states) =>
          states.evalScan(Initial: Status) {
            case (Initial, cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              val leader = cluster.members(leaderId)
              (leaderId to (leaderId + size / 2))
                .map(_ % size)
                .toList
                .traverse(rafts(_).disconnect)
                .as(Reign(0, leaderId, leader.currentTerm))
            case (Reign(0, prevLeader, prevTerm), cluster)
                if cluster.hasNoLeader & cluster
                  .members
                  .exists(_.currentTerm >= prevTerm + 2) =>
              ((prevLeader + 1) to (prevLeader + size / 2))
                .map(_ % size)
                .toList
                .traverse(rafts(_).connect)
                .as(Epoch(1))
            case (Epoch(1), cluster) if cluster.soleLeaderId.isDefined =>
              IO.pure(Target)
            case (status, _) => IO.pure(status)
          }
      }

      "recover from catastrophic failure".withSize in createCluster(size).flatMap {
        case (rafts, states) =>
          states.evalScan(Initial: Status) {
            case (Initial, _) =>
              rafts.traverse(_.disconnect) as Epoch(0)
            case (Epoch(0), cluster)
                if cluster.hasNoLeader & cluster.members.exists(_.currentTerm >= 2) =>
              rafts.traverse(_.connect) as Epoch(1)
            case (Epoch(1), cluster) if cluster.soleLeaderId.isDefined =>
              IO.pure(Target)
            case (status, _) => IO.pure(status)
          }
      }

      "ignore an old leader".withSize in createCluster(size).flatMap {
        case (rafts, states) =>
          states.evalScan(Initial: Status) {
            case (Initial, cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              val leader = cluster.members(leaderId)
              rafts(leaderId).disconnect.as(Reign(0, leaderId, leader.currentTerm))
            case (Reign(0, prevLeader, _), cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              val leader = cluster.members(leaderId)
              rafts(prevLeader).connect.as(Reign(1, leaderId, leader.currentTerm))
            case (Reign(1, expectedLeader, expectedTerm), cluster)
                if cluster.soleLeaderId.exists { id =>
                  id == expectedLeader && cluster.members(id).currentTerm == expectedTerm
                } =>
              IO.pure(Target)
            case (status, _) => IO.pure(status)
          }
      }

      "run election when follower returns".withSize in createCluster(size).flatMap {
        case (rafts, states) =>
          states.evalScan(Initial: Status) {
            case (Initial, cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              val leader = cluster.members(leaderId)
              rafts((leaderId + 1) % size).disconnect.as(Reign(0, leaderId, leader.currentTerm))
            case (Reign(0, leaderId, currentTerm), cluster)
                if cluster.members.exists(_.currentTerm >= currentTerm + 2) =>
              rafts((leaderId + 1) % size).connect.as(Reign(1, leaderId, currentTerm))
            case (Reign(1, _, prevTerm), cluster)
                if cluster.soleLeaderId.exists(cluster.members(_).currentTerm > prevTerm) =>
              IO.pure(Target)
            case (status, _) => IO.pure(status)
          }
      }

      "commit one command".withSize in createCluster(size).flatMap {
        case (rafts, states) =>
          val command = ByteString.copyFromUtf8("meow")

          val commit = rafts
            .map(_.raft.commit)
            .foldRight(Stream.repeatEval(IO.pure(List.empty[ByteString]))) { (commit, acc) =>
              commit.zipWith(acc)(_ :: _)
            }
            .collect { case x if x.forall(_ == command) => Target: Status }
            .head
            .repeat
            .merge(Stream.repeatEval(IO.pure(Initial)))

          val state = states.evalScan(Initial: Status) {
            case (Initial, cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              rafts(leaderId).raft.command1(command).as(Epoch(0))
            case (Epoch(0), cluster) if cluster.isCommitted(command) =>
              IO.pure(Target)
            case (status, _) =>
              IO.pure(status)
          }

          state.zipWith(commit) {
            case (Target, Target) => Target: Status
            case _ => Initial: Status
          }
      }

      "commit one command submitted to non-leader".withSize in createCluster(size).flatMap {
        case (rafts, states) =>
          val command = ByteString.copyFromUtf8("meow")

          val commit = rafts
            .map(_.raft.commit)
            .foldRight(Stream.repeatEval(IO.pure(List.empty[ByteString]))) { (commit, acc) =>
              commit.zipWith(acc)(_ :: _)
            }
            .collect { case x if x.forall(_ == command) => Target: Status }
            .head
            .repeat
            .merge(Stream.repeatEval(IO.pure(Initial)))

          val state = states.evalScan(Initial: Status) {
            case (Initial, cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              rafts((leaderId + 1) % size).raft.command1(command).as(Epoch(0))
            case (Epoch(0), cluster) if cluster.isCommitted(command) =>
              IO.pure(Target)
            case (status, _) =>
              IO.pure(status)
          }

          state.zipWith(commit) {
            case (Target, Target) => Target: Status
            case _ => Initial: Status
          }
      }

      "commit multiple commands".withSize in createCluster(size).flatMap {
        case (rafts, states) =>
          val commands = List("meee", "owww", "!!!").map(ByteString.copyFromUtf8)

          val commit = rafts
            .map(_.raft.commit.scan(List.empty[ByteString])(_ :+ _))
            .foldRight(Stream.repeatEval(IO.pure(List.empty[List[ByteString]])).repeat) { (commit, acc) =>
              commit.zipWith(acc)(_ :: _)
            }
            .collect { case x if x.forall(_ == commands) => Target: Status }
            .head
            .repeat
            .merge(Stream.repeatEval(IO.pure(Initial)))

          val state = states.evalScan(Initial: Status) {
            case (Initial, cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              rafts(leaderId).raft.command(Stream.emits(commands)).compile.drain.as(Epoch(0))
            case (Epoch(0), cluster) if cluster.isCommitted(commands.last) =>
              IO.pure(Target)
            case (status, _) =>
              IO.pure(status)
          }

          state.zipWith(commit) {
            case (Target, Target) => Target: Status
            case _ => Initial: Status
          }
      }
    }
  }

}
