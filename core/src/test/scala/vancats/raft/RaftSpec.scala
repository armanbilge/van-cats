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
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.specs2.mutable.Specification
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
  final case object Target extends Status
  final case class Reign(epoch: Int, leader: Int, term: Int) extends Status

  final case class ClusterState(members: Map[Int, State[IO]]) {

    def soleLeaderId: Option[Int] =
      Option.when(members.values.count(_.role.isLeader) == 1)(
        members.find(_._2.role.isLeader).get._1)

    def hasNoLeader: Boolean = members.values.count(_.role.isLeader) == 0

  }
}

class RaftSpec extends Specification with CatsEffect {

  override val Timeout: FiniteDuration = 10.seconds

  def createCluster(size: Int): IO[(Seq[TestRaft], Stream[IO, ClusterState])] =
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
              GrpcRaft.Config(100.milliseconds, 1.seconds, 0.5))
            _ <- deferred.complete(GrpcRaft.flaky(raft, connected))
            stream = raft
              .state
              .foreach { s =>
                connected.get.flatMap {
                  if (_)
                    signal.update(_.updated(id, s))
                  else
                    signal.update(_.removed(id))
                }
              }
              .drain
          } yield (
            TestRaft(raft, connected.get, connected.set(true), connected.set(false)),
            stream)
        }
      }
      testRafts = rafts.map(_._1)
      stream = rafts
        .foldLeft(signal.discrete)((acc, x) => acc.concurrently(x._2))
        .map(ClusterState)
    } yield (testRafts, stream)

  "GrpcRaft" should {

    "elect a single leader" in createCluster(3).flatMap {
      case (_, states) =>
        states.find(_.soleLeaderId.isDefined).compile.lastOrError.as(success).timeout(Timeout)
    }

    "elect a new leader" in createCluster(3).flatMap {
      case (rafts, states) =>
        states
          .evalScan(List.empty[Status]) {
            case (Nil, cluster) if cluster.soleLeaderId.isDefined =>
              val leaderId = cluster.soleLeaderId.get
              val leader = cluster.members(leaderId)
              rafts(leaderId).disconnect.as(Reign(0, leaderId, leader.currentTerm) :: Nil)
            case (Reign(0, prevLeader, prevTerm) :: _, cluster) if cluster.soleLeaderId.exists {
                  id => id != prevLeader && cluster.members(id).currentTerm > prevTerm
                } =>
              IO.pure(Target :: Nil)
            case (status, _) => IO.pure(status)
          }
          .collectFirst { case Target :: _ => () }
          .compile
          .lastOrError
          .as(success)
          .timeout(Timeout)
    }
  }

}
