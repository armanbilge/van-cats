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
import com.comcast.ip4s.SocketAddress
import com.comcast.ip4s.IpAddress
import fs2.Chunk
import fs2.Stream
import fs2.io.net.Datagram
import fs2.io.net.DatagramSocket
import fs2.io.net.DatagramSocketGroup
import scodec.Codec
import scodec.Decoder
import scodec.Encoder
import scodec.bits.BitVector

final case class DatagramChannelAddress[-A](socket: SocketAddress[IpAddress], channel: String)

object DatagramRemoteChannel:

  final private case class WireMessage(to: String, data: BitVector) derives Codec

  def apply[F[_]: Concurrent](
      using
      sg: DatagramSocketGroup[F]): Resource[F, RemoteChannel.Aux[F, DatagramChannelAddress]] =
    sg.openDatagramSocket(None, None, Nil, None).flatMap(apply[F])

  def apply[F[_]](socket: DatagramSocket[F])(
      using F: Concurrent[F]): Resource[F, RemoteChannel.Aux[F, DatagramChannelAddress]] =
    for
      (t1, t2) <- F.product(F.unique, F.unique).toResource
      channelCounter <- F.ref(t1.hashCode.toLong << 32 | t2.hashCode.toLong).toResource
      channels <- F.ref(Map.empty[String, Queue[F, BitVector]]).toResource
      _ <- socket
        .reads
        .parEvalMapUnordered(Int.MaxValue) {
          case Datagram(_, bytes) =>
            for
              WireMessage(to, data) <- F.fromTry(
                Decoder[WireMessage].decode(bytes.toBitVector).toTry.map(_.value))
              channels <- channels.get
              _ <- channels.get(to).fold(F.unit)(_.offer(data))
            yield ()
        }
        .compile
        .drain
        .background
    yield new RemoteChannel[F]:
      type ChannelAddress[X] = DatagramChannelAddress[X]

      def send[A: Codec](to: ChannelAddress[A], a: A) =
        Stream.emit(a).through(sendAll(to)).compile.drain

      def sendAll[A: Codec](to: ChannelAddress[A]) = (_: Stream[F, A])
        .chunks
        .evalMap { as =>
          for
            payload <- F.fromTry(Codec[List[A]].encode(as.toList).toTry)
            wireMessage <- F.fromTry(
              Encoder[WireMessage].encode(WireMessage(to.channel, payload)).toTry)
          yield Datagram(to.socket, Chunk.byteVector(wireMessage.toByteVector))
        }
        .through(socket.writes)

      def mkChannel[A: Codec]: Resource[F, (ChannelAddress[A], Stream[F, A])] =
        for
          id <- channelCounter.modifyState(SplitMix64).toResource
          address <- mkChannel(id.toHexString)
        yield address

      def mkChannel[A: Codec](id: String): Resource[F, (ChannelAddress[A], Stream[F, A])] =
        Resource.make {
          for
            queue <- Queue.unbounded[F, BitVector]
            _ <- channels.modify { channels =>
              if channels.contains(id) then
                (channels, Left(new RuntimeException(s"Channel id `${id}` is already in use")))
              else (channels.updated(id, queue), Right(()))
            }.rethrow
            address <- socket.localAddress.map(DatagramChannelAddress(_, id))
            stream: Stream[F, A] = Stream
              .fromQueueUnterminated(queue)
              .evalMap(b => F.fromTry(Codec[List[A]].decode(b).toTry.map(_.value)))
              .flatMap(Stream.emits(_))
          yield (address, stream)
        } { (address, _) => channels.update(_.removed(address.channel)) }
