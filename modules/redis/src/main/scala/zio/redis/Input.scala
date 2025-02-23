/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
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

package zio.redis

import zio._
import zio.redis.internal.{RespCommand, RespCommandArgument}
import zio.schema.codec.BinaryCodec

import java.time.Instant
import java.util.concurrent.TimeUnit

sealed trait Input[-A] { self =>
  private[redis] def encode(data: A): RespCommand

  final def contramap[B](f: B => A): Input[B] =
    new Input[B] {
      def encode(data: B): RespCommand = self.encode(f(data))
    }
}

object Input {
  def apply[A](implicit input: Input[A]): Input[A] = input

  case object AbsTtlInput extends Input[AbsTtl] {
    def encode(data: AbsTtl): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object AddressInput extends Input[Address] {
    def encode(data: Address): RespCommand =
      RespCommand(RespCommandArgument.Value(data.asString))
  }

  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate): RespCommand =
      RespCommand(RespCommandArgument.Literal("AGGREGATE"), RespCommandArgument.Literal(data.asString))
  }

  case object AlphaInput extends Input[Alpha] {
    def encode(data: Alpha): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  final case class ArbitraryKeyInput[A: BinaryCodec]() extends Input[A] {
    def encode(data: A): RespCommand =
      RespCommand(RespCommandArgument.Key(data))
  }

  final case class ArbitraryValueInput[A: BinaryCodec]() extends Input[A] {
    def encode(data: A): RespCommand =
      RespCommand(RespCommandArgument.Value(data))
  }

  case object AuthInput extends Input[Auth] {
    def encode(data: Auth): RespCommand =
      data.username match {
        case Some(username) =>
          RespCommand(RespCommandArgument.Value(username), RespCommandArgument.Value(data.password))
        case None           => RespCommand(RespCommandArgument.Value(data.password))
      }
  }

  case object BitFieldCommandInput extends Input[BitFieldCommand] {
    def encode(data: BitFieldCommand): RespCommand = {
      import BitFieldCommand._

      val respArgs = data match {
        case BitFieldGet(t, o)     =>
          Chunk(
            RespCommandArgument.Literal("GET"),
            RespCommandArgument.Value(t.asString),
            RespCommandArgument.Value(o.toString)
          )
        case BitFieldSet(t, o, v)  =>
          Chunk(
            RespCommandArgument.Literal("SET"),
            RespCommandArgument.Value(t.asString),
            RespCommandArgument.Value(o.toString),
            RespCommandArgument.Value(v.toString)
          )
        case BitFieldIncr(t, o, i) =>
          Chunk(
            RespCommandArgument.Literal("INCRBY"),
            RespCommandArgument.Value(t.asString),
            RespCommandArgument.Value(o.toString),
            RespCommandArgument.Value(i.toString)
          )
        case bfo: BitFieldOverflow =>
          Chunk(RespCommandArgument.Literal("OVERFLOW"), RespCommandArgument.Literal(bfo.asString))
      }
      RespCommand(respArgs)
    }
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange): RespCommand = {
      val start    = RespCommandArgument.Value(data.start.toString)
      val respArgs = data.end.fold(Chunk.single(start))(end => Chunk(start, RespCommandArgument.Value(end.toString)))
      RespCommand(respArgs)
    }
  }

  case object BlockInput extends Input[Duration] {
    def encode(data: Duration): RespCommand =
      RespCommand(RespCommandArgument.Literal("BLOCK"), RespCommandArgument.Value(data.toMillis.toString))
  }

  case object DbInput extends Input[Long] {
    def encode(db: Long): RespCommand =
      RespCommand(RespCommandArgument.Literal("DB"), RespCommandArgument.Value(db.toString))
  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean): RespCommand =
      RespCommand(RespCommandArgument.Literal(if (data) "1" else "0"))
  }

  case object ByInput extends Input[String] {
    def encode(data: String): RespCommand =
      RespCommand(RespCommandArgument.Literal("BY"), RespCommandArgument.Value(data))
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object CommandNameInput extends Input[String] {
    def encode(data: String): RespCommand =
      RespCommand(RespCommandArgument.CommandName(data))
  }

  case object CopyInput extends Input[Copy] {
    def encode(data: Copy): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count): RespCommand =
      RespCommand(RespCommandArgument.Literal("COUNT"), RespCommandArgument.Value(data.count.toString))
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double): RespCommand =
      RespCommand(RespCommandArgument.Value(data.toString))
  }

  case object DurationMillisecondsInput extends Input[Duration] {
    def encode(data: Duration): RespCommand =
      RespCommand(RespCommandArgument.Value(data.toMillis.toString))
  }

  case object DurationSecondsInput extends Input[Duration] {
    def encode(data: Duration): RespCommand = {
      val seconds = TimeUnit.MILLISECONDS.toSeconds(data.toMillis)
      RespCommand(RespCommandArgument.Value(seconds.toString))
    }
  }

  case object DurationTtlInput extends Input[Duration] {
    def encode(data: Duration): RespCommand = {
      val milliseconds = data.toMillis
      RespCommand(RespCommandArgument.Literal("PX"), RespCommandArgument.Value(milliseconds.toString))
    }
  }

  final case class EvalInput[-K, -V](inputK: Input[K], inputV: Input[V]) extends Input[(String, Chunk[K], Chunk[V])] {
    def encode(data: (String, Chunk[K], Chunk[V])): RespCommand = {
      val (lua, keys, args) = data
      val encodedScript     = RespCommand(RespCommandArgument.Value(lua), RespCommandArgument.Value(keys.size.toString))

      val encodedKeys =
        keys.foldLeft(RespCommand.empty) { (acc, a) =>
          acc ++ inputK.encode(a).mapArguments(arg => RespCommandArgument.Key(arg.value.value))
        }

      val encodedArgs =
        args.foldLeft(RespCommand.empty) { (acc, a) =>
          acc ++ inputV.encode(a).mapArguments(arg => RespCommandArgument.Value(arg.value.value))
        }

      encodedScript ++ encodedKeys ++ encodedArgs
    }
  }

  case object FreqInput extends Input[Freq] {
    def encode(data: Freq): RespCommand =
      RespCommand(RespCommandArgument.Literal("FREQ"), RespCommandArgument.Value(data.frequency))
  }

  case object GetInput extends Input[String] {
    def encode(data: String): RespCommand =
      RespCommand(RespCommandArgument.Literal("GET"), RespCommandArgument.Value(data))
  }

  final case class GetExInput[K: BinaryCodec]() extends Input[(K, GetExpire)] {
    def encode(data: (K, GetExpire)): RespCommand =
      data match {
        case (key, GetExpire.Milliseconds(milliseconds))         =>
          RespCommand(
            RespCommandArgument.Key(key),
            RespCommandArgument.Literal("PX"),
            RespCommandArgument.Value(milliseconds.toString)
          )
        case (key, GetExpire.Persist)                            =>
          RespCommand(RespCommandArgument.Key(key), RespCommandArgument.Literal("PERSIST"))
        case (key, GetExpire.Seconds(seconds))                   =>
          RespCommand(
            RespCommandArgument.Key(key),
            RespCommandArgument.Literal("EX"),
            RespCommandArgument.Value(seconds.toString)
          )
        case (key, GetExpire.UnixTimeMilliseconds(milliseconds)) =>
          RespCommand(
            RespCommandArgument.Key(key),
            RespCommandArgument.Literal("PXAT"),
            RespCommandArgument.Value(milliseconds.toString)
          )
        case (key, GetExpire.UnixTimeSeconds(seconds))           =>
          RespCommand(
            RespCommandArgument.Key(key),
            RespCommandArgument.Literal("EXAT"),
            RespCommandArgument.Value(seconds.toString)
          )
      }
  }

  case object GetKeywordInput extends Input[GetKeyword] {
    def encode(data: GetKeyword): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object IdleInput extends Input[Duration] {
    def encode(data: Duration): RespCommand =
      RespCommand(RespCommandArgument.Literal("IDLE"), RespCommandArgument.Value(data.toMillis.toString))
  }

  case object IdleTimeInput extends Input[IdleTime] {
    def encode(data: IdleTime): RespCommand =
      RespCommand(RespCommandArgument.Literal("IDLETIME"), RespCommandArgument.Value(data.seconds.toString))
  }

  case object IdInput extends Input[Long] {
    def encode(data: Long): RespCommand =
      RespCommand(RespCommandArgument.Literal("ID"), RespCommandArgument.Value(data.toString))
  }

  case object IdsInput extends Input[(Long, List[Long])] {
    def encode(data: (Long, List[Long])): RespCommand =
      RespCommand(
        Chunk.fromIterable(
          RespCommandArgument.Literal("ID") +: (data._1 :: data._2).map(id => RespCommandArgument.Value(id.toString))
        )
      )
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object IntInput extends Input[Int] {
    def encode(data: Int): RespCommand =
      RespCommand(RespCommandArgument.Value(data.toString))
  }

  case object LcsQueryTypeInput extends Input[LcsQueryType] {
    def encode(data: LcsQueryType): RespCommand = data match {
      case LcsQueryType.Len                                  => RespCommand(RespCommandArgument.Literal("LEN"))
      case LcsQueryType.Idx(minMatchLength, withMatchLength) =>
        val idx = Chunk.single(RespCommandArgument.Literal("IDX"))

        val min =
          if (minMatchLength > 1)
            Chunk(RespCommandArgument.Literal("MINMATCHLEN"), RespCommandArgument.Value(minMatchLength.toString))
          else Chunk.empty[RespCommandArgument]

        val length =
          if (withMatchLength) Chunk.single(RespCommandArgument.Literal("WITHMATCHLEN"))
          else Chunk.empty[RespCommandArgument]

        RespCommand(Chunk(idx, min, length).flatten)
    }
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("LIMIT"),
        RespCommandArgument.Value(data.offset.toString),
        RespCommandArgument.Value(data.count.toString)
      )
  }

  case object ListMaxLenInput extends Input[ListMaxLen] {
    def encode(data: ListMaxLen): RespCommand =
      RespCommand(RespCommandArgument.Literal("MAXLEN"), RespCommandArgument.Value(data.count.toString))
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long): RespCommand =
      RespCommand(RespCommandArgument.Value(data.toString))
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat): RespCommand =
      RespCommand(
        RespCommandArgument.Value(data.longitude.toString),
        RespCommandArgument.Value(data.latitude.toString)
      )
  }

  final case class MemberScoreInput[M: BinaryCodec]() extends Input[MemberScore[M]] {
    def encode(data: MemberScore[M]): RespCommand = {
      val score = data.score match {
        case Double.NegativeInfinity => "-inf"
        case Double.PositiveInfinity => "+inf"
        case d: Double               => d.toString.toLowerCase
      }
      RespCommand(RespCommandArgument.Value(score), RespCommandArgument.Value(data.member))
    }
  }

  case object NoAckInput extends Input[NoAck] {
    def encode(data: NoAck): RespCommand =
      RespCommand(RespCommandArgument.Value(data.asString))
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit): RespCommand = RespCommand.empty
  }

  final case class NonEmptyList[-A](input: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A])): RespCommand =
      (data._1 :: data._2).foldLeft(RespCommand.empty)((acc, a) => acc ++ input.encode(a))
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A]): RespCommand =
      data.fold(RespCommand.empty)(a.encode)
  }

  case object OrderInput extends Input[Order] {
    def encode(data: Order): RespCommand =
      RespCommand(RespCommandArgument.Value(data.asString))
  }

  case object PatternInput extends Input[Pattern] {
    def encode(data: Pattern): RespCommand =
      RespCommand(RespCommandArgument.Literal("MATCH"), RespCommandArgument.Value(data.pattern))
  }

  case object PositionInput extends Input[Position] {
    def encode(data: Position): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object RadiusUnitInput extends Input[RadiusUnit] {
    def encode(data: RadiusUnit): RespCommand =
      RespCommand(RespCommandArgument.Value(data.asString))
  }

  case object RangeInput extends Input[Range] {
    def encode(data: Range): RespCommand = {
      val end = if (data.isInclusive) data.end else data.end - 1
      RespCommand(RespCommandArgument.Value(data.start.toString), RespCommandArgument.Value(end.toString))
    }
  }

  case object RankInput extends Input[Rank] {
    def encode(data: Rank): RespCommand =
      RespCommand(RespCommandArgument.Literal("RANK"), RespCommandArgument.Value(data.rank.toString))
  }

  case object RedisTypeInput extends Input[RedisType] {
    def encode(data: RedisType): RespCommand =
      RespCommand(RespCommandArgument.Literal("TYPE"), RespCommandArgument.Literal(data.asString))
  }

  case object ReplaceInput extends Input[Replace] {
    def encode(data: Replace): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object RetryCountInput extends Input[Long] {
    def encode(data: Long): RespCommand =
      RespCommand(RespCommandArgument.Literal("RETRYCOUNT"), RespCommandArgument.Value(data.toString))
  }

  case object ScriptDebugInput extends Input[DebugMode] {
    def encode(data: DebugMode): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object ScriptFlushInput extends Input[FlushMode] {
    def encode(data: FlushMode): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object SetExpireInput extends Input[SetExpire] {
    def encode(data: SetExpire): RespCommand =
      data match {
        case SetExpire.KeepTtl                            =>
          RespCommand(RespCommandArgument.Literal("KEEPTTL"))
        case SetExpire.Milliseconds(milliseconds)         =>
          RespCommand(RespCommandArgument.Literal("PX"), RespCommandArgument.Value(milliseconds.toString))
        case SetExpire.Seconds(seconds)                   =>
          RespCommand(RespCommandArgument.Literal("EX"), RespCommandArgument.Value(seconds.toString))
        case SetExpire.UnixTimeMilliseconds(milliseconds) =>
          RespCommand(RespCommandArgument.Literal("PXAT"), RespCommandArgument.Value(milliseconds.toString))
        case SetExpire.UnixTimeSeconds(seconds)           =>
          RespCommand(RespCommandArgument.Literal("EXAT"), RespCommandArgument.Value(seconds.toString))
      }
  }

  case object SideInput extends Input[Side] {
    def encode(data: Side): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object StoreDistInput extends Input[StoreDist] {
    def encode(data: StoreDist): RespCommand =
      RespCommand(RespCommandArgument.Literal("STOREDIST"), RespCommandArgument.Value(data.key))
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store): RespCommand =
      RespCommand(RespCommandArgument.Literal("STORE"), RespCommandArgument.Value(data.key))
  }

  case object MaxLenApproxInput extends Input[CappedStreamType.MaxLenApprox] {
    def encode(data: CappedStreamType.MaxLenApprox): RespCommand = {
      val maxLenChunk: Chunk[RespCommandArgument] = Chunk(
        RespCommandArgument.Literal("MAXLEN"),
        RespCommandArgument.Literal("~"),
        RespCommandArgument.Value(data.count.toString)
      )

      val limitChunk = data.limit.fold(Chunk.empty[RespCommandArgument])(limit =>
        Chunk(RespCommandArgument.Literal("LIMIT"), RespCommandArgument.Value(limit.toString))
      )

      RespCommand(maxLenChunk ++ limitChunk)
    }
  }

  case object MaxLenExactInput extends Input[CappedStreamType.MaxLenExact] {
    def encode(data: CappedStreamType.MaxLenExact): RespCommand =
      RespCommand(
        Chunk(
          RespCommandArgument.Literal("MAXLEN"),
          RespCommandArgument.Literal("="),
          RespCommandArgument.Value(data.count.toString)
        )
      )
  }

  final case class MinIdApproxInput[I: BinaryCodec]() extends Input[CappedStreamType.MinIdApprox[I]] {
    def encode(data: CappedStreamType.MinIdApprox[I]): RespCommand = {
      val minIdChunk =
        Chunk(
          RespCommandArgument.Literal("MINID"),
          RespCommandArgument.Literal("~"),
          RespCommandArgument.Value(data.id)
        )

      val limitChunk = data.limit.fold(Chunk.empty[RespCommandArgument])(limit =>
        Chunk(RespCommandArgument.Literal("LIMIT"), RespCommandArgument.Value(limit.toString))
      )

      RespCommand(minIdChunk ++ limitChunk)
    }
  }

  final case class MinIdExactInput[I: BinaryCodec]() extends Input[CappedStreamType.MinIdExact[I]] {
    def encode(data: _root_.zio.redis.CappedStreamType.MinIdExact[I]): RespCommand =
      RespCommand(
        Chunk(
          RespCommandArgument.Literal("MINID"),
          RespCommandArgument.Literal("="),
          RespCommandArgument.Value(data.id)
        )
      )
  }

  case object MkStreamInput extends Input[MkStream] {
    def encode(data: MkStream): RespCommand =
      RespCommand(RespCommandArgument.Value(data.asString))
  }

  case object NoMkStreamInput extends Input[NoMkStream] {
    def encode(data: NoMkStream): RespCommand =
      RespCommand(RespCommandArgument.Value(data.asString))
  }

  final case class StreamsInput[K: BinaryCodec, V: BinaryCodec]() extends Input[((K, V), Chunk[(K, V)])] {
    def encode(data: ((K, V), Chunk[(K, V)])): RespCommand = {
      val (keys, ids) = (data._1 +: data._2).map { case (key, value) =>
        (RespCommandArgument.Key(key), RespCommandArgument.Value(value))
      }.unzip

      RespCommand(Chunk.single(RespCommandArgument.Literal("STREAMS")) ++ keys ++ ids)
    }
  }

  case object StringInput extends Input[String] {
    def encode(data: String): RespCommand =
      RespCommand(RespCommandArgument.Value(data))
  }

  case object TimeInput extends Input[Duration] {
    def encode(data: Duration): RespCommand =
      RespCommand(RespCommandArgument.Literal("TIME"), RespCommandArgument.Value(data.toMillis.toString))
  }

  case object TimeMillisecondsInput extends Input[Instant] {
    def encode(data: Instant): RespCommand =
      RespCommand(RespCommandArgument.Value(data.toEpochMilli.toString))
  }

  case object TimeSecondsInput extends Input[Instant] {
    def encode(data: Instant): RespCommand =
      RespCommand(RespCommandArgument.Value(data.getEpochSecond.toString))
  }

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)] {
    def encode(data: (A, B)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2)
  }

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)] {
    def encode(data: (A, B, C)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3)
  }

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)] {
    def encode(data: (A, B, C, D)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4)
  }

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)] {
    def encode(data: (A, B, C, D, E)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5)
  }

  final case class Tuple6[-A, -B, -C, -D, -E, -F](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F]
  ) extends Input[(A, B, C, D, E, F)] {
    def encode(data: (A, B, C, D, E, F)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5) ++
        _6.encode(data._6)
  }

  final case class Tuple7[-A, -B, -C, -D, -E, -F, -G](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G]
  ) extends Input[(A, B, C, D, E, F, G)] {
    def encode(data: (A, B, C, D, E, F, G)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5) ++
        _6.encode(data._6) ++ _7.encode(data._7)
  }

  final case class Tuple9[-A, -B, -C, -D, -E, -F, -G, -H, -I](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G],
    _8: Input[H],
    _9: Input[I]
  ) extends Input[(A, B, C, D, E, F, G, H, I)] {
    def encode(data: (A, B, C, D, E, F, G, H, I)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5) ++
        _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++ _9.encode(data._9)
  }

  final case class Tuple10[-A, -B, -C, -D, -E, -F, -G, -H, -I, -J](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G],
    _8: Input[H],
    _9: Input[I],
    _10: Input[J]
  ) extends Input[(A, B, C, D, E, F, G, H, I, J)] {
    def encode(data: (A, B, C, D, E, F, G, H, I, J)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++
        _5.encode(data._5) ++ _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++
        _9.encode(data._9) ++ _10.encode(data._10)
  }

  final case class Tuple11[-A, -B, -C, -D, -E, -F, -G, -H, -I, -J, -K](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G],
    _8: Input[H],
    _9: Input[I],
    _10: Input[J],
    _11: Input[K]
  ) extends Input[(A, B, C, D, E, F, G, H, I, J, K)] {
    def encode(data: (A, B, C, D, E, F, G, H, I, J, K)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++
        _5.encode(data._5) ++ _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++
        _9.encode(data._9) ++ _10.encode(data._10) ++ _11.encode(data._11)
  }

  case object UpdateInput extends Input[Update] {
    def encode(data: Update): RespCommand =
      RespCommand(RespCommandArgument.Value(data.asString))
  }

  case object UpdateByScoreInput extends Input[UpdateByScore] {
    def encode(data: UpdateByScore): RespCommand =
      RespCommand(RespCommandArgument.Value(data.asString))
  }

  case object ValueInput extends Input[Chunk[Byte]] {
    def encode(data: Chunk[Byte]): RespCommand =
      RespCommand(RespCommandArgument.Value(data))
  }

  final case class Varargs[-A](input: Input[A]) extends Input[Iterable[A]] {
    def encode(data: Iterable[A]): RespCommand =
      data.foldLeft(RespCommand.empty)((acc, a) => acc ++ input.encode(a))
  }

  case object WeightsInput extends Input[::[Double]] {
    def encode(data: ::[Double]): RespCommand = {
      val args = data.foldLeft(Chunk.single[RespCommandArgument](RespCommandArgument.Literal("WEIGHTS"))) { (acc, a) =>
        acc ++ Chunk.single(RespCommandArgument.Value(a.toString))
      }
      RespCommand(args)
    }
  }

  case object WithCoordInput extends Input[WithCoord] {
    def encode(data: WithCoord): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object WithDistInput extends Input[WithDist] {
    def encode(data: WithDist): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object WithForceInput extends Input[WithForce] {
    def encode(data: WithForce): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  final case class LastIdInput[I: BinaryCodec]() extends Input[LastId[I]] {
    def encode(data: LastId[I]): RespCommand = {
      val chunk = Chunk(RespCommandArgument.Literal("LASTID"), RespCommandArgument.Value(data.lastId))
      RespCommand(chunk)
    }
  }

  case object WithHashInput extends Input[WithHash] {
    def encode(data: WithHash): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object WithJustIdInput extends Input[WithJustId] {
    def encode(data: WithJustId): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object WithScoreInput extends Input[WithScore] {
    def encode(data: WithScore): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object WithScoresInput extends Input[WithScores] {
    def encode(data: WithScores): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.asString))
  }

  case object WithEntriesReadInput extends Input[WithEntriesRead] {
    def encode(data: WithEntriesRead): RespCommand =
      RespCommand(RespCommandArgument.Literal("ENTRIESREAD"), RespCommandArgument.Value(data.entries.toString))
  }

  final case class XGroupCreateConsumerInput[K: BinaryCodec, G: BinaryCodec, C: BinaryCodec]()
      extends Input[XGroupCommand.CreateConsumer[K, G, C]] {
    def encode(data: XGroupCommand.CreateConsumer[K, G, C]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("CREATECONSUMER"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group),
        RespCommandArgument.Value(data.consumer)
      )
  }

  final case class XGroupCreateInput[K: BinaryCodec, G: BinaryCodec, I: BinaryCodec]()
      extends Input[XGroupCommand.Create[K, G, I]] {
    def encode(data: XGroupCommand.Create[K, G, I]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("CREATE"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group),
        RespCommandArgument.Value(data.id)
      )
  }

  final case class XGroupDelConsumerInput[K: BinaryCodec, G: BinaryCodec, C: BinaryCodec]()
      extends Input[XGroupCommand.DelConsumer[K, G, C]] {
    def encode(data: XGroupCommand.DelConsumer[K, G, C]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("DELCONSUMER"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group),
        RespCommandArgument.Value(data.consumer)
      )
  }

  final case class XGroupDestroyInput[K: BinaryCodec, G: BinaryCodec]() extends Input[XGroupCommand.Destroy[K, G]] {
    def encode(data: XGroupCommand.Destroy[K, G]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("DESTROY"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group)
      )
  }

  final case class XGroupSetIdInput[K: BinaryCodec, G: BinaryCodec, I: BinaryCodec]()
      extends Input[XGroupCommand.SetId[K, G, I]] {
    def encode(data: XGroupCommand.SetId[K, G, I]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("SETID"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group),
        RespCommandArgument.Value(data.id)
      )

  }
}
