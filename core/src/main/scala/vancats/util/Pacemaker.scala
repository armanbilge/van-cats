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

package vancats.util

import cats.effect.Temporal
import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

sealed abstract private[vancats] class Pacemaker[F[_]] {
  def start: F[Unit]
  def stop: F[Unit]
  def reset: F[Unit]
  def stream: Stream[F, Unit]
}

private[vancats] object Pacemaker {

  def apply[F[_]: Temporal](rate: FiniteDuration): F[Pacemaker[F]] =
    for {
      semaphore <- Semaphore[F](1)
      _ <- semaphore.acquire
      inc <- Temporal[F].ref(0)
    } yield new Pacemaker[F] {

      override def start: F[Unit] = for {
        expectedInc <- inc.updateAndGet(_ + 1)
        _ <- inc
          .get
          .flatMap(inc =>
            if (inc == expectedInc)
              semaphore.release
            else
              Temporal[F].unit)
          .delayBy(rate)
      } yield ()

      override def stop: F[Unit] =
        inc.update(_ + 1) *> semaphore.acquire

      override def reset: F[Unit] = stop *> start

      override def stream: Stream[F, Unit] =
        Stream
          .fixedRate(rate, dampen = true)
          .zipLeft(Stream.repeatEval(semaphore.permit.use(_.pure)))
    }
}
