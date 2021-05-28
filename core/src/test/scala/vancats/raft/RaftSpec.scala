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
import vancats.raft.RaftSpec.TestRaft

import scala.concurrent.duration._

object RaftSpec {
  final case class TestRaft(raft: GrpcRaft[IO], freeze: IO[Unit], unfreeze: IO[Unit])
}

class RaftSpec extends Specification with CatsEffect {

  override val Timeout: FiniteDuration = 10.seconds

  def createCluster(size: Int): IO[(Seq[TestRaft], Stream[IO, Seq[State]])] =
    for {
      deferredServers <- Vector.fill(size)(Deferred[IO, GrpcRaft[IO]]).sequence
      servers = deferredServers.map(GrpcRaft.deferred[IO])
      signal <- SignallingRef[IO, Seq[State]](Seq.fill(3)(State()))
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
      stream = rafts.foldLeft(signal.discrete)((acc, x) => acc.concurrently(x._2))
    } yield (testRafts, stream)

  "GrpcRaft" should {

    "elect a single leader" in {
      createCluster(3).flatMap {
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
    }

    "elect a new leader" in {
      for {
        (rafts, states) <- createCluster(3)
        firstLeader <- Deferred[IO, (Int, Int)]
        _ <- firstLeader.get.flatMap(l => rafts(l._1).freeze).start
        success <- states
          .evalTap { state =>
            if (state.count(_.role.isLeader) == 1) {
              val leader = state.indexWhere(_.role.isLeader)
              val term = state(leader).currentTerm
              firstLeader.complete((leader, term))
            } else IO.unit
          }
          .prefetchN(Int.MaxValue)
          .zip(Stream.repeatEval(firstLeader.get))
          .find {
            case (state, (_, prevTerm)) =>
              val alive = state.filter(_.currentTerm > prevTerm)
              alive.count(_.role.isLeader) == 1 & alive.count(_.role.isFollower) == 1
            case _ => false
          }
          .compile
          .lastOrError
          .as(success)
          .timeout(Timeout)
      } yield success
    }
  }

}
