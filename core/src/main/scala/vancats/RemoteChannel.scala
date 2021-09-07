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
import scodec.Iso
import scodec.bits.BitVector
import scodec.bits.ByteVector

trait RemoteChannel[F[_]]:
  type Address
  type ChannelAddress[-_]

  def send[A: Codec](to: ChannelAddress[A], a: A): F[Unit]

  def sendAll[A: Codec](to: ChannelAddress[A]): Pipe[F, A, INothing]

  def mkChannel[A: Codec]: Resource[F, (ChannelAddress[A], Stream[F, A])]

  def mkChannel[A: Codec](id: String): Resource[F, (ChannelAddress[A], Stream[F, A])]
  
object RemoteChannel:
  type Aux[F[_], C[-_]] = RemoteChannel[F] { type ChannelAddress[X] = C[X] }
