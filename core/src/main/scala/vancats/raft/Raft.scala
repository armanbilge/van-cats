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

import cats.Monad
import cats.effect.std.Random
import cats.effect.{Deferred, Temporal}
import cats.syntax.all._
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import fs2.concurrent.Channel
import fs2.{Pipe, Stream}
import io.grpc.Metadata
import vancats.raft.ConsensusModule.{AppendEntries, RequestVote, ServiceClient, State}

import scala.concurrent.duration.FiniteDuration

sealed abstract class Raft[F[_], C] {
  def command: Pipe[F, C, Nothing]
  def command1(c: C): F[Unit]
  def commit: Stream[F, C]
  def state: Stream[F, State[F]]
}

sealed abstract class GrpcRaft[F[_]] extends Raft[F, ByteString] with RaftFs2Grpc[F, Metadata]

object GrpcRaft {

  final case class Config(
      heartRate: FiniteDuration,
      electionTimeout: FiniteDuration,
      electionTimeoutNoiseFactor: Double
  )

  def apply[F[_]: Temporal: Random](
      id: Int,
      peers: Map[Int, RaftFs2Grpc[F, Metadata]],
      config: Config): F[GrpcRaft[F]] = for {
    commitChannel <- Channel.unbounded[F, ByteString]
    server <- ConsensusModule(
      id,
      peers.size + 1,
      config.heartRate,
      config.electionTimeout,
      config.electionTimeoutNoiseFactor,
      peers,
      (_: Stream[F, ByteString]).evalMap(commitChannel.send).drain
    )
  } yield new GrpcRaft[F] {

    override def command: Pipe[F, ByteString, Nothing] =
      _.map(ServiceClient[F]).through(server.command)

    override def command1(c: ByteString): F[Unit] =
      server.command1(ServiceClient(c))

    override def commit: Stream[F, ByteString] =
      commitChannel.stream

    override def state: Stream[F, State[F]] =
      server.state

    override def requestVote(request: RequestVoteRequest, ctx: Metadata): F[RequestVoteReply] =
      Deferred[F, RequestVoteReply].flatMap { reply =>
        server.command1(RequestVote(request, reply)) *> reply.get
      }

    override def requestVoteStream(
        request: Stream[F, RequestVoteRequest],
        ctx: Metadata): Stream[F, RequestVoteReply] =
      request.evalMap(requestVote(_, ctx))

    override def appendEntries(
        request: AppendEntriesRequest,
        ctx: Metadata): F[AppendEntriesReply] =
      Deferred[F, AppendEntriesReply].flatMap { reply =>
        server.command1(AppendEntries(request, reply)) *> reply.get
      }

    override def appendEntriesStream(
        request: Stream[F, AppendEntriesRequest],
        ctx: Metadata): Stream[F, AppendEntriesReply] =
      request.evalMap(appendEntries(_, ctx))

    override def serviceClient(request: ServiceClientRequest, ctx: Metadata): F[Empty] =
      server.command1(ServiceClient(request.command)) *> Empty().pure

    override def serviceClientStream(
        request: Stream[F, ServiceClientRequest],
        ctx: Metadata): Stream[F, Empty] =
      request.evalMap(serviceClient(_, ctx))

  }

  private[raft] def deferred[F[_]: Monad](underlying: Deferred[F, GrpcRaft[F]]): GrpcRaft[F] =
    new GrpcRaft[F] {
      override def command: Pipe[F, ByteString, Nothing] =
        in => Stream.eval(underlying.get).flatMap(_.command(in))

      override def command1(c: ByteString): F[Unit] =
        underlying.get.flatMap(_.command1(c))

      override def commit: Stream[F, ByteString] =
        Stream.eval(underlying.get).flatMap(_.commit)

      override def state: Stream[F, State[F]] =
        Stream.eval(underlying.get).flatMap(_.state)

      override def requestVote(
          request: RequestVoteRequest,
          ctx: Metadata): F[RequestVoteReply] =
        underlying.get.flatMap(_.requestVote(request, ctx))

      override def requestVoteStream(
          request: Stream[F, RequestVoteRequest],
          ctx: Metadata): Stream[F, RequestVoteReply] =
        Stream.eval(underlying.get).flatMap(_.requestVoteStream(request, ctx))

      override def appendEntries(
          request: AppendEntriesRequest,
          ctx: Metadata): F[AppendEntriesReply] =
        underlying.get.flatMap(_.appendEntries(request, ctx))

      override def appendEntriesStream(
          request: Stream[F, AppendEntriesRequest],
          ctx: Metadata): Stream[F, AppendEntriesReply] =
        Stream.eval(underlying.get).flatMap(_.appendEntriesStream(request, ctx))

      override def serviceClient(request: ServiceClientRequest, ctx: Metadata): F[Empty] =
        underlying.get.flatMap(_.serviceClient(request, ctx))

      override def serviceClientStream(
          request: Stream[F, ServiceClientRequest],
          ctx: Metadata): Stream[F, Empty] =
        Stream.eval(underlying.get).flatMap(_.serviceClientStream(request, ctx))
    }
}
