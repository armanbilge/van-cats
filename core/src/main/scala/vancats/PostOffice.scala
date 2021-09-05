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

import cats.ApplicativeThrow
import cats.effect.kernel.Concurrent
import cats.effect.kernel.Resource
import cats.effect.std.Queue
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.comcast.ip4s.Host
import com.comcast.ip4s.Hostname
import com.comcast.ip4s.IDN
import com.comcast.ip4s.IpAddress
import com.comcast.ip4s.Ipv4Address
import com.comcast.ip4s.Ipv6Address
import com.comcast.ip4s.Port
import com.comcast.ip4s.SocketAddress
import fs2.Chunk
import fs2.INothing
import fs2.Pipe
import fs2.Stream
import fs2.io.net.Datagram
import fs2.io.net.DatagramSocket
import fs2.io.net.DatagramSocketGroup
import scodec.Attempt
import scodec.Codec
import scodec.Decoder
import scodec.Encoder
import scodec.bits.BitVector
import scodec.bits.ByteVector

trait PostOffice[F[_]] extends CollectionBox[F], PoBoxProvider[F]

trait CollectionBox[F[_]]:
  def address: F[SocketAddress[IpAddress]]
  def sendLetter[A: Encoder](to: PoBoxAddress[A], message: A): F[Unit]
  def sendLetters[A: Encoder]: Pipe[F, Letter[A], INothing]

trait PoBoxProvider[F[_]]:
  def leaseBox[A: Decoder]: Resource[F, (PoBoxAddress[A], Stream[F, A])]
  def leaseBox[A: Decoder](name: String): Resource[F, (PoBoxAddress[A], Stream[F, A])]

final case class Letter[A](to: PoBoxAddress[A], message: A):
  private[vancats] def toWire[F[_]](using F: ApplicativeThrow[F], e: Encoder[A]) =
    F.fromTry(e.encode(message).toTry).map(WireLetter(to.box, _))

final private[vancats] case class WireLetter(to: String, message: BitVector) derives Codec

final case class PoBoxAddress[-A](postOffice: SocketAddress[IpAddress], box: String)
    derives Codec

object DatagramPostOffice:
  def apply[F[_]: Concurrent](using sg: DatagramSocketGroup[F]): Resource[F, PostOffice[F]] =
    sg.openDatagramSocket(None, None, Nil, None).flatMap(apply[F])

  def apply[F[_]](socket: DatagramSocket[F])(
      using F: Concurrent[F]): Resource[F, PostOffice[F]] =
    for
      (t1, t2) <- F.product(F.unique, F.unique).toResource
      boxCounter <- F.ref(t1.hashCode.toLong << 32 | t2.hashCode.toLong).toResource
      boxes <- F.ref(Map.empty[String, Queue[F, BitVector]]).toResource
      _ <- socket
        .reads
        .parEvalMapUnordered(Int.MaxValue) {
          case Datagram(_, bytes) =>
            for
              WireLetter(to, message) <- F.fromTry(
                Decoder[WireLetter].decode(bytes.toBitVector).toTry.map(_.value))
              boxes <- boxes.get
              _ <- boxes.get(to).fold(F.unit)(_.offer(message))
            yield ()
        }
        .compile
        .drain
        .background
    yield new PostOffice[F]:
      def address = socket.localAddress

      def sendLetter[A: Encoder](to: PoBoxAddress[A], message: A) =
        Stream.emit(Letter(to, message)).through(sendLetters).compile.drain

      def sendLetters[A: Encoder] = (_: Stream[F, Letter[A]])
        .evalMap { letter =>
          for
            wireLetter <- letter.toWire[F]
            encoded <- F.fromTry(Encoder[WireLetter].encode(wireLetter).toTry)
          yield Datagram(letter.to.postOffice, Chunk.byteVector(encoded.toByteVector))
        }
        .through(socket.writes)

      def leaseBox[A: Decoder]: Resource[F, (PoBoxAddress[A], Stream[F, A])] =
        for
          id <- boxCounter.modifyState(SplitMix64).toResource
          box <- leaseBox(id.toHexString)
        yield box

      def leaseBox[A](name: String)(
          using decoder: Decoder[A]): Resource[F, (PoBoxAddress[A], Stream[F, A])] =
        Resource.make {
          for
            queue <- Queue.unbounded[F, BitVector]
            _ <- boxes.modify { boxes =>
              if boxes.contains(name) then
                (boxes, Left(new RuntimeException(s"Name `${name}` is already leased")))
              else (boxes.updated(name, queue), Right(()))
            }.rethrow
            address <- address.map(PoBoxAddress(_, name))
            stream: Stream[F, A] = Stream
              .fromQueueUnterminated(queue)
              .evalMap(b => F.fromTry(decoder.decode(b).toTry.map(_.value)))
          yield (address, stream)
        } { (address, _) => boxes.update(_.removed(address.box)) }
