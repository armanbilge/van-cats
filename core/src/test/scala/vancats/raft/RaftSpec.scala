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

import cats.effect.std.{Random, Semaphore}
import cats.effect.testing.specs2.CatsEffect
import cats.effect.{Deferred, IO}
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.specs2.mutable.Specification
import vancats.raft.ConsensusModule.State
import vancats.raft.RaftSpec.TestRaft

import scala.concurrent.duration._

object RaftSpec {
  final case class TestRaft(raft: GrpcRaft[IO], freeze: IO[Unit], unfreeze: IO[Unit])
}

class RaftSpec extends Specification with CatsEffect {

  override val Timeout: FiniteDuration = 10.seconds

  def createCluster(size: Int): IO[(Seq[TestRaft], Stream[IO, Seq[State[IO]]])] =
    for {
      deferredServers <- Vector.fill(size)(Deferred[IO, GrpcRaft[IO]]).sequence
      servers = deferredServers.map(GrpcRaft.deferred[IO])
      signal <- SignallingRef[IO, Map[Int, State[IO]]](Map.empty)
      rafts <- deferredServers.traverseWithIndexM { (deferred, id) =>
        val peers = servers.zipWithIndex.map(_.swap).toMap.removed(id)
        Random.scalaUtilRandom[IO].flatMap { implicit random =>
          for {
            raft <- GrpcRaft[IO](id, peers, GrpcRaft.Config(100.milliseconds, 1.seconds, 0.5))
            _ <- deferred.complete(raft)
            switch <- Semaphore[IO](1)
            stream = raft
              .state
              .evalTap(_ => switch.permit.use(_ => IO.unit))
              .foreach(s => signal.update(_.updated(id, s)))
              .drain
          } yield (TestRaft(raft, switch.acquire, switch.release), stream)
        }
      }
      testRafts = rafts.map(_._1)
      stream = rafts.foldLeft(signal.discrete)((acc, x) => acc.concurrently(x._2)).collect {
        case state if state.size == size => Seq.tabulate(3)(state)
      }
    } yield (testRafts, stream)

  "GrpcRaft" should {

    "elect a single leader" in createCluster(3).flatMap {
      case (_, states) =>
        states
          .find { state =>
            state.count(_.role.isLeader) == 1 && state.count(_.role.isFollower) == 2
          }
          .compile
          .lastOrError
          .as(success)
          .timeout(Timeout)
    }

    "elect a new leader" in createCluster(3).flatMap {
      case (rafts, states) =>
        sealed abstract class Status
        case object Initial extends Status
        case class FirstLegitTerm(term: Int) extends Status
        case object SecondLegitTerm extends Status

        states
          .evalScan(Initial: Status) {
            case (Initial, state)
                if state.count(_.role.isLeader) == 1 && state.count(_.role.isFollower) == 2 =>
              val leaderId = state.indexWhere(_.role.isLeader)
              rafts(leaderId)
                .freeze
                .as(FirstLegitTerm(state.find(_.role.isLeader).get.currentTerm))
            case (FirstLegitTerm(prevTerm), state) if {
                  val alive = state.filter(_.currentTerm > prevTerm)
                  alive.count(_.role.isLeader) == 1 && alive.count(_.role.isFollower) == 1
                } =>
              IO.pure(SecondLegitTerm)
            case (status, _) => IO.pure(status)
          }
          .collectFirst { case SecondLegitTerm => SecondLegitTerm }
          .compile
          .lastOrError
          .as(success)
          .timeout(Timeout)
    }
  }

}
