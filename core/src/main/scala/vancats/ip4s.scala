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

import com.comcast.ip4s.Host
import com.comcast.ip4s.Hostname
import com.comcast.ip4s.IDN
import com.comcast.ip4s.IpAddress
import com.comcast.ip4s.Ipv4Address
import com.comcast.ip4s.Ipv6Address
import com.comcast.ip4s.Port
import com.comcast.ip4s.SocketAddress
import scodec.Attempt
import scodec.Codec
import scodec.Decoder
import scodec.Encoder
import scodec.Err
import scodec.bits.ByteVector
import scodec.codecs

private[vancats] given Codec[SocketAddress[IpAddress]] =
  Codec[(IpAddress, Port)].xmap(SocketAddress.apply, a => (a.host, a.port))
private[vancats] given Codec[IpAddress] = codecs
  .discriminated[IpAddress]
  .by(codecs.bool(8))
  .caseP(false) { case ip: Ipv4Address => ip }(identity)(Codec[Ipv4Address])
  .caseP(true) { case ip: Ipv6Address => ip }(identity)(Codec[Ipv6Address])
private[vancats] given Codec[Ipv6Address] = codecs
  .bytes(16)
  .xmap(b => Ipv6Address.fromBytes(b.toArray).get, a => ByteVector.view(a.toBytes))
private[vancats] given Codec[Ipv4Address] =
  codecs.int32.xmap(b => Ipv4Address.fromLong(b), _.toLong.toInt)
private[vancats] given Codec[Port] = codecs.uint16.xmap(Port.fromInt(_).get, _.value)
