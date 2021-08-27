package zio.redis

import java.time.Instant
import java.util.concurrent.TimeUnit

import zio.Chunk
import zio.duration.Duration
import zio.schema.Schema
import zio.schema.codec.Codec

sealed trait Input[-A] {
  private[redis] def encode(data: A)(implicit codec: Codec): Chunk[RespValue.BulkString]
}

object Input {

  @inline
  private[this] def encodeString(s: String): RespValue.BulkString = RespValue.bulkString(s)
  @inline
  private[this] def encodeBytes[A](value: A)(implicit codec: Codec, schema: Schema[A]): RespValue.BulkString =
    RespValue.BulkString(codec.encode(schema)(value))

  case object AbsTtlInput extends Input[AbsTtl] {
    def encode(data: AbsTtl)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object AddressInput extends Input[Address] {
    def encode(data: Address)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("AGGREGATE"), encodeString(data.stringify))
  }

  case object AlphaInput extends Input[Alpha] {
    def encode(data: Alpha)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object AuthInput extends Input[Auth] {
    def encode(data: Auth)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("AUTH"), encodeString(data.password))
  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(if (data) "1" else "0"))
  }

  case object StralgoLcsQueryTypeInput extends Input[StrAlgoLcsQueryType] {
    def encode(data: StrAlgoLcsQueryType)(implicit codec: Codec): Chunk[RespValue.BulkString] = data match {
      case StrAlgoLcsQueryType.Len => Chunk.single(encodeString("LEN"))
      case StrAlgoLcsQueryType.Idx(minMatchLength, withMatchLength) => {
        val idx = Chunk.single(encodeString("IDX"))
        val min =
          if (minMatchLength > 1) Chunk(encodeString("MINMATCHLEN"), encodeString(minMatchLength.toString))
          else Chunk.empty
        val length = if (withMatchLength) Chunk.single(encodeString("WITHMATCHLEN")) else Chunk.empty
        Chunk(idx, min, length).flatten
      }
    }
  }

  case object BitFieldCommandInput extends Input[BitFieldCommand] {
    def encode(data: BitFieldCommand)(implicit codec: Codec): Chunk[RespValue.BulkString] = {
      import BitFieldCommand._

      data match {
        case BitFieldGet(t, o) => Chunk(encodeString("GET"), encodeString(t.stringify), encodeString(o.toString))
        case BitFieldSet(t, o, v) =>
          Chunk(encodeString("SET"), encodeString(t.stringify), encodeString(o.toString), encodeString(v.toString))
        case BitFieldIncr(t, o, i) =>
          Chunk(encodeString("INCRBY"), encodeString(t.stringify), encodeString(o.toString), encodeString(i.toString))
        case bfo: BitFieldOverflow => Chunk(encodeString("OVERFLOW"), encodeString(bfo.stringify))
      }
    }
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange)(implicit codec: Codec): Chunk[RespValue.BulkString] = {
      val start = encodeString(data.start.toString)
      data.end.fold(Chunk.single(start))(end => Chunk(start, encodeString(end.toString)))
    }
  }

  case object ByInput extends Input[String] {
    def encode(data: String)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("BY"), encodeString(data))
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object ClientKillInput extends Input[ClientKillFilter] {
    def encode(data: ClientKillFilter)(implicit codec: Codec): Chunk[RespValue.BulkString] = data match {
      case addr: ClientKillFilter.Address       => Chunk(encodeString("ADDR"), encodeString(addr.stringify))
      case laddr: ClientKillFilter.LocalAddress => Chunk(encodeString("LADDR"), encodeString(laddr.stringify))
      case ClientKillFilter.Id(clientId)        => Chunk(encodeString("ID"), encodeString(clientId.toString))
      case ClientKillFilter.Type(clientType)    => Chunk(encodeString("TYPE"), encodeString(clientType.stringify))
      case ClientKillFilter.User(username)      => Chunk(encodeString("USER"), encodeString(username))
      case ClientKillFilter.SkipMe(skip)        => Chunk(encodeString("SKIPME"), encodeString(if (skip) "YES" else "NO"))
    }
  }

  case object ClientPauseModeInput extends Input[ClientPauseMode] {
    def encode(data: ClientPauseMode)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object ClientTrackingInput
      extends Input[Option[(Option[Long], Option[ClientTrackingMode], Boolean, Chunk[String])]] {
    override private[redis] def encode(
      data: Option[(Option[Long], Option[ClientTrackingMode], Boolean, Chunk[String])]
    )(implicit codec: Codec): Chunk[RespValue.BulkString] =
      data match {
        case Some((clientRedir, mode, noLoop, prefixes)) =>
          val modeChunk = mode match {
            case Some(ClientTrackingMode.OptIn)     => Chunk(encodeString("OPTIN"))
            case Some(ClientTrackingMode.OptOut)    => Chunk(encodeString("OPTOUT"))
            case Some(ClientTrackingMode.Broadcast) => Chunk(encodeString("BCAST"))
            case None                               => Chunk.empty
          }
          val loopChunk = if (noLoop) Chunk(encodeString("NOLOOP")) else Chunk.empty
          Chunk(encodeString("ON")) ++
            clientRedir.map(id => Chunk(encodeString("REDIRECT"), encodeString(id.toString))).getOrElse(Chunk.empty) ++
            prefixes.flatMap(prefix => Chunk(encodeString("PREFIX"), encodeString(prefix))) ++
            modeChunk ++
            loopChunk
        case None =>
          Chunk(encodeString("OFF"))
      }
  }

  case object ClientTypeInput extends Input[ClientType] {
    def encode(data: ClientType)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("TYPE"), encodeString(data.stringify))
  }

  case object CopyInput extends Input[Copy] {
    def encode(data: Copy)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("COUNT"), encodeString(data.count.toString))
  }

  case object RedisTypeInput extends Input[RedisType] {
    def encode(data: RedisType)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("TYPE"), encodeString(data.stringify))
  }

  case object PatternInput extends Input[Pattern] {
    def encode(data: Pattern)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("MATCH"), encodeString(data.pattern))
  }

  case object GetInput extends Input[String] {
    def encode(data: String)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("GET"), encodeString(data))
  }

  case object PositionInput extends Input[Position] {
    def encode(data: Position)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object SideInput extends Input[Side] {
    def encode(data: Side)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.toString))
  }

  case object DurationMillisecondsInput extends Input[Duration] {
    def encode(data: Duration)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.toMillis.toString))
  }

  case object DurationSecondsInput extends Input[Duration] {
    def encode(data: Duration)(implicit codec: Codec): Chunk[RespValue.BulkString] = {
      val seconds = TimeUnit.MILLISECONDS.toSeconds(data.toMillis)
      Chunk.single(encodeString(seconds.toString))
    }
  }

  case object DurationTtlInput extends Input[Duration] {
    def encode(data: Duration)(implicit codec: Codec): Chunk[RespValue.BulkString] = {
      val milliseconds = data.toMillis
      Chunk(encodeString("PX"), encodeString(milliseconds.toString))
    }
  }

  case object FreqInput extends Input[Freq] {
    def encode(data: Freq)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("FREQ"), encodeString(data.frequency))
  }

  case object IdleTimeInput extends Input[IdleTime] {
    def encode(data: IdleTime)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("IDLETIME"), encodeString(data.seconds.toString))
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object KeepTtlInput extends Input[KeepTtl] {
    def encode(data: KeepTtl)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("LIMIT"), encodeString(data.offset.toString), encodeString(data.count.toString))
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.toString))
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString(data.longitude.toString), encodeString(data.latitude.toString))
  }

  final case class MemberScoreInput[M: Schema]() extends Input[MemberScore[M]] {
    def encode(data: MemberScore[M])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString(data.score.toString), encodeBytes(data.member))
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit)(implicit codec: Codec): Chunk[RespValue.BulkString] = Chunk.empty
  }

  final case class NonEmptyList[-A](input: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A]))(implicit codec: Codec): Chunk[RespValue.BulkString] =
      (data._1 :: data._2).foldLeft(Chunk.empty: Chunk[RespValue.BulkString])((acc, a) => acc ++ input.encode(a))
  }

  case object OrderInput extends Input[Order] {
    def encode(data: Order)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object RadiusUnitInput extends Input[RadiusUnit] {
    def encode(data: RadiusUnit)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object RangeInput extends Input[Range] {
    def encode(data: Range)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString(data.start.toString), encodeString(data.end.toString))
  }

  case object ReplaceInput extends Input[Replace] {
    def encode(data: Replace)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object StoreDistInput extends Input[StoreDist] {
    def encode(data: StoreDist)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("STOREDIST"), encodeString(data.key))
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("STORE"), encodeString(data.key))
  }

  case object StringInput extends Input[String] {
    def encode(data: String)(implicit codec: Codec): Chunk[RespValue.BulkString] = Chunk.single(encodeString(data))
  }

  final case class ArbitraryInput[A: Schema]() extends Input[A] {
    def encode(data: A)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeBytes(data))
  }

  case object ByteInput extends Input[Chunk[Byte]] {
    def encode(data: Chunk[Byte])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(RespValue.BulkString(data))
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      data.fold(Chunk.empty: Chunk[RespValue.BulkString])(a.encode)
  }

  case object TimeSecondsInput extends Input[Instant] {
    def encode(data: Instant)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.getEpochSecond.toString))
  }

  case object TimeMillisecondsInput extends Input[Instant] {
    def encode(data: Instant)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.toEpochMilli.toString))
  }

  case object WeightsInput extends Input[::[Double]] {
    def encode(data: ::[Double])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      data.foldLeft(Chunk.single(encodeString("WEIGHTS")): Chunk[RespValue.BulkString])((acc, a) =>
        acc ++ Chunk.single(encodeString(a.toString))
      )
  }

  case object IdleInput extends Input[Duration] {
    def encode(data: Duration)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("IDLE"), encodeString(data.toMillis.toString))
  }

  case object TimeInput extends Input[Duration] {
    def encode(data: Duration)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("TIME"), encodeString(data.toMillis.toString))
  }

  case object RetryCountInput extends Input[Long] {
    def encode(data: Long)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("RETRYCOUNT"), encodeString(data.toString))
  }

  final case class XGroupCreateInput[K: Schema, G: Schema, I: Schema]() extends Input[XGroupCommand.Create[K, G, I]] {
    def encode(data: XGroupCommand.Create[K, G, I])(implicit codec: Codec): Chunk[RespValue.BulkString] = {
      val chunk = Chunk(encodeString("CREATE"), encodeBytes(data.key), encodeBytes(data.group), encodeBytes(data.id))

      if (data.mkStream) chunk :+ encodeString(MkStream.stringify)
      else chunk
    }
  }

  final case class XGroupSetIdInput[K: Schema, G: Schema, I: Schema]() extends Input[XGroupCommand.SetId[K, G, I]] {
    def encode(data: XGroupCommand.SetId[K, G, I])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("SETID"), encodeBytes(data.key), encodeBytes(data.group), encodeBytes(data.id))
  }

  final case class XGroupDestroyInput[K: Schema, G: Schema]() extends Input[XGroupCommand.Destroy[K, G]] {
    def encode(data: XGroupCommand.Destroy[K, G])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("DESTROY"), encodeBytes(data.key), encodeBytes(data.group))
  }

  final case class XGroupCreateConsumerInput[K: Schema, G: Schema, C: Schema]()
      extends Input[XGroupCommand.CreateConsumer[K, G, C]] {
    def encode(data: XGroupCommand.CreateConsumer[K, G, C])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("CREATECONSUMER"), encodeBytes(data.key), encodeBytes(data.group), encodeBytes(data.consumer))
  }

  final case class XGroupDelConsumerInput[K: Schema, G: Schema, C: Schema]()
      extends Input[XGroupCommand.DelConsumer[K, G, C]] {
    def encode(data: XGroupCommand.DelConsumer[K, G, C])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("DELCONSUMER"), encodeBytes(data.key), encodeBytes(data.group), encodeBytes(data.consumer))
  }

  case object BlockInput extends Input[Duration] {
    def encode(data: Duration)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("BLOCK"), encodeString(data.toMillis.toString))
  }

  final case class StreamsInput[K: Schema, V: Schema]() extends Input[((K, V), Chunk[(K, V)])] {
    def encode(data: ((K, V), Chunk[(K, V)]))(implicit codec: Codec): Chunk[RespValue.BulkString] = {
      val (keys, ids) = (data._1 +: data._2).map { case (key, value) =>
        (encodeBytes(key), encodeBytes(value))
      }.unzip

      Chunk.single(encodeString("STREAMS")) ++ keys ++ ids
    }
  }

  case object NoAckInput extends Input[NoAck] {
    def encode(data: NoAck)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object StreamMaxLenInput extends Input[StreamMaxLen] {
    def encode(data: StreamMaxLen)(implicit codec: Codec): Chunk[RespValue.BulkString] = {
      val chunk =
        if (data.approximate)
          Chunk(encodeString("MAXLEN"), encodeString("~"))
        else
          Chunk.single(encodeString("MAXLEN"))

      chunk :+ encodeString(data.count.toString)
    }
  }

  case object ListMaxLenInput extends Input[ListMaxLen] {
    def encode(data: ListMaxLen)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("MAXLEN"), encodeString(data.count.toString))
  }

  case object RankInput extends Input[Rank] {
    def encode(data: Rank)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("RANK"), encodeString(data.rank.toString))
  }

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)] {
    def encode(data: (A, B))(implicit codec: Codec): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2)
  }

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)] {
    def encode(data: (A, B, C))(implicit codec: Codec): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3)
  }

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)] {
    def encode(data: (A, B, C, D))(implicit codec: Codec): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4)
  }

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)] {
    def encode(data: (A, B, C, D, E))(implicit codec: Codec): Chunk[RespValue.BulkString] =
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
    def encode(data: (A, B, C, D, E, F))(implicit codec: Codec): Chunk[RespValue.BulkString] =
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
    def encode(data: (A, B, C, D, E, F, G))(implicit codec: Codec): Chunk[RespValue.BulkString] =
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
    def encode(data: (A, B, C, D, E, F, G, H, I))(implicit codec: Codec): Chunk[RespValue.BulkString] =
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
    def encode(data: (A, B, C, D, E, F, G, H, I, J))(implicit codec: Codec): Chunk[RespValue.BulkString] =
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
    def encode(data: (A, B, C, D, E, F, G, H, I, J, K))(implicit codec: Codec): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++
        _5.encode(data._5) ++ _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++
        _9.encode(data._9) ++ _10.encode(data._10) ++ _11.encode(data._11)
  }

  case object UpdateInput extends Input[Update] {
    def encode(data: Update)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  final case class GetExPersistInput[K: Schema]() extends Input[(K, Boolean)] {
    def encode(data: (K, Boolean))(implicit codec: Codec): Chunk[RespValue.BulkString] =
      if (data._2) Chunk(encodeBytes(data._1), encodeString("PERSIST")) else Chunk(encodeBytes(data._1))
  }
  final case class GetExInput[K: Schema]() extends Input[(K, Expire, Duration)] {
    def encode(
      data: (K, Expire, Duration)
    )(implicit codec: Codec): Chunk[RespValue.BulkString] =
      data match {
        case (key, Expire.SetExpireSeconds, duration) =>
          Chunk(encodeBytes(key), encodeString("EX")) ++ DurationSecondsInput.encode(duration)
        case (key, Expire.SetExpireMilliseconds, duration) =>
          Chunk(encodeBytes(key), encodeString("PX")) ++ DurationMillisecondsInput.encode(duration)
        case _ => Chunk(encodeBytes(data._1))
      }
  }

  final case class GetExAtInput[K: Schema]() extends Input[(K, ExpiredAt, Instant)] {
    def encode(
      data: (K, ExpiredAt, Instant)
    )(implicit codec: Codec): Chunk[RespValue.BulkString] =
      data match {
        case (key, ExpiredAt.SetExpireAtSeconds, instant) =>
          Chunk(encodeBytes(key), encodeString("EXAT")) ++ TimeSecondsInput.encode(instant)
        case (key, ExpiredAt.SetExpireAtMilliseconds, instant) =>
          Chunk(encodeBytes(key), encodeString("PXAT")) ++ TimeMillisecondsInput.encode(instant)
        case _ => Chunk(encodeBytes(data._1))
      }
  }

  case object IdInput extends Input[Long] {
    def encode(data: Long)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk(encodeString("ID"), encodeString(data.toString))
  }

  case object UnblockBehaviorInput extends Input[UnblockBehavior] {
    def encode(data: UnblockBehavior)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  final case class Varargs[-A](input: Input[A]) extends Input[Iterable[A]] {
    def encode(data: Iterable[A])(implicit codec: Codec): Chunk[RespValue.BulkString] =
      data.foldLeft(Chunk.empty: Chunk[RespValue.BulkString])((acc, a) => acc ++ input.encode(a))
  }

  case object WithScoresInput extends Input[WithScores] {
    def encode(data: WithScores)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object WithCoordInput extends Input[WithCoord] {
    def encode(data: WithCoord)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object WithDistInput extends Input[WithDist] {
    def encode(data: WithDist)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object WithHashInput extends Input[WithHash] {
    def encode(data: WithHash)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object WithForceInput extends Input[WithForce] {
    def encode(data: WithForce)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object WithJustIdInput extends Input[WithJustId] {
    def encode(data: WithJustId)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(data.stringify))
  }

  case object YesNoInput extends Input[Boolean] {
    def encode(data: Boolean)(implicit codec: Codec): Chunk[RespValue.BulkString] =
      Chunk.single(encodeString(if (data) "YES" else "NO"))
  }
}
