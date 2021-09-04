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
import fs2.INothing
import fs2.Pipe
import fs2.Stream
import fs2.io.net.Datagram
import fs2.io.net.DatagramSocket
import fs2.io.net.DatagramSocketGroup
import scodec.Attempt
import scodec.Codec
import scodec.bits.ByteVector
import fs2.Chunk

trait PostOffice[F[_]] extends CollectionBox[F], POBox[F]

trait CollectionBox[F[_]]:
  def address: F[SocketAddress[IpAddress]]
  def sendLetter[A: Codec](to: Address, message: A): F[Unit]
  def sendLetters[A: Codec]: Pipe[F, Letter[A], INothing]

trait POBox[F[_]]:
  def leaseBox[A: Codec]: Resource[F, (Address, Stream[F, A])]
  def leaseBox[A: Codec](name: String): Resource[F, (Address, Stream[F, A])]

final case class Letter[+A](to: Address, message: A):
  private[vancats] def toLocal = LocalLetter(to.box, message)
final private[vancats] case class LocalLetter[+A](to: String, message: A) derives Codec

final case class Address(postOffice: SocketAddress[IpAddress], box: String) derives Codec

object DatagramPostOffice:
  def apply[F[_]: Concurrent](using sg: DatagramSocketGroup[F]): Resource[F, PostOffice[F]] =
    sg.openDatagramSocket(None, None, Nil, None).flatMap(apply[F])

  def apply[F[_]](socket: DatagramSocket[F])(
      using F: Concurrent[F]): Resource[F, PostOffice[F]] =
    for
      boxCounter <- F.ref(0L).toResource
      boxes <- F.ref(Map.empty[String, Queue[F, ByteVector]]).toResource
    yield new PostOffice[F]:
      def address = socket.localAddress

      def sendLetter[A: Codec](to: Address, message: A) =
        Stream.emit(Letter(to, message)).through(sendLetters).compile.drain

      def sendLetters[A: Codec] = (_: Stream[F, Letter[A]])
        .evalMap { letter =>
          for encoded <- F.fromTry(Codec[LocalLetter[A]].encode(letter.toLocal).toTry)
          yield Datagram(letter.to.postOffice, Chunk.byteVector(encoded.toByteVector))
        }
        .through(socket.writes)

      def leaseBox[A: Codec]: Resource[F, (Address, Stream[F, A])] = ???
      def leaseBox[A: Codec](name: String): Resource[F, (Address, Stream[F, A])] = ???
