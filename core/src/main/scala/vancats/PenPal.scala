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

package vancats

import cats.effect.kernel.Resource
import cats.effect.kernel.Temporal
import cats.syntax.all.*
import fs2.INothing
import fs2.Pipe
import fs2.Stream
import scodec.Codec
import scodec.Decoder
import scodec.Encoder
import scodec.bits.BitVector

import scala.concurrent.duration.*

trait PenPal[F[_]]:

  def writeLetters[A: Encoder]: Pipe[F, A, LetterWriter[A]]
  def writeLettersTo[A: Encoder](reader: LetterReader[A]): Pipe[F, A, INothing]

  def readLetters[A: Decoder]: Resource[F, (LetterReader[A], Stream[F, A])]
  def readLettersFrom[A: Decoder](writer: LetterWriter[A]): Stream[F, A]

final case class LetterWriter[+A](address: PoBoxAddress[LetterReader.Message[A]]) derives Codec
object LetterWriter:
  enum Message[+A] derives Codec:
    case ReadMe[+A](writer: LetterWriter[A]) extends Message[A]
    case ReadMyLetters[+A](letters: List[A]) extends Message[A]
    case StopReading extends Message[Nothing]
    case Busy extends Message[Nothing]

  final private[vancats] case class State[A](peer: Option[LetterReader[A]], communicationEstablished: Boolean)

final case class LetterReader[-A](address: PoBoxAddress[LetterWriter.Message[A]]) derives Codec
object LetterReader:
  enum Message[-A]:
    case WriteToMe[-A](reader: LetterReader[A]) extends Message[A]
    case WriteMoreToMe extends Message[Any]
    case StopWriting extends Message[Any]
    case Busy extends Message[Any]

  private given Codec[Unit] = scodec.codecs.bits(0).xmap(_ => (), _ => BitVector.empty)
  given [A]: Codec[Message[A]] =
    Codec.derived[Message[Unit]].xmap(_.asInstanceOf[Message[A]], _.asInstanceOf[Message[Unit]])

  final private[vancats] case class State[A](
      peer: Option[LetterWriter[A]] = None,
      establishedCommunication: Boolean = false)

object PenPal:
  def apply[F[_]: Temporal](
      postOffice: PostOffice[F],
      heartRate: FiniteDuration = 1.second): PenPal[F] =
    new PenPal[F]:

      def writeLetters[A: Encoder](state: LetterWriter.State[A]): Pipe[F, A, LetterWriter[A]] =
        in =>
          for
            (address, messages) <- Stream.resource(postOffice.leaseBox[LetterReader.Message[A]])
            _ = messages.evalMapAccumulate(state) {
              case (state @ LetterWriter.State(Some(_), true), message) => (state, message)
              case (LetterWriter.State(None, _), LetterReader.Message.WriteToMe(reader)) =>

            }
            out <- Stream.emit(LetterWriter(address))
          yield out

      def writeLetters[A: Encoder]: Pipe[F, A, LetterWriter[A]] = in =>
        for (address, messages) <- Stream.resource(postOffice.leaseBox[LetterReader.Message[A]])
        yield LetterWriter(address)

      def writeLettersTo[A: Encoder](reader: LetterReader[A]): Pipe[F, A, INothing] =
        ???

      def readLetters[A: Decoder]: Resource[F, (LetterReader[A], Stream[F, A])] = ???

      def readLettersFrom[A: Decoder](writer: LetterWriter[A]): Stream[F, A] = ???

      def readLetters[A: Codec](
          state: LetterReader.State[A]): Resource[F, (LetterReader[A], Stream[F, A])] =
        for (address, messages) <- postOffice.leaseBox[LetterWriter.Message[A]]
        yield ???
