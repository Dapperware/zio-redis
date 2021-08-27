package zio.redis

import java.net.InetAddress
import java.time.Instant

import zio.duration._
import zio.redis.Input._
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Task }

object InputSpec extends BaseSpec {
  import StrAlgoLcsQueryType._
  import BitFieldCommand._
  import BitFieldType._
  import BitOperation._
  import Order._
  import RadiusUnit._

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Input encoders")(
      suite("AbsTtl")(
        testM("valid value") {
          for {
            result <- Task(AbsTtlInput.encode(AbsTtl))
          } yield assert(result)(equalTo(respArgs("ABSTTL")))
        }
      ),
      suite("Address")(
        testM("valid value") {
          val ip   = InetAddress.getByName("127.0.0.1")
          val port = 42
          for {
            result <- Task(AddressInput.encode(Address(ip, port)))
          } yield assert(result)(equalTo(respArgs("127.0.0.1:42")))
        }
      ),
      suite("Aggregate")(
        testM("max") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Max))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "MAX")))
        },
        testM("min") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Min))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "MIN")))
        },
        testM("sum") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Sum))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "SUM")))
        }
      ),
      suite("Alpha")(
        testM("alpha") {
          for {
            result <- Task(AlphaInput.encode(Alpha))
          } yield assert(result)(equalTo(respArgs("ALPHA")))
        }
      ),
      suite("Auth")(
        testM("with empty password") {
          for {
            result <- Task(AuthInput.encode(Auth("")))
          } yield assert(result)(equalTo(respArgs("AUTH", "")))
        },
        testM("with non-empty password") {
          for {
            result <- Task(AuthInput.encode(Auth("pass")))
          } yield assert(result)(equalTo(respArgs("AUTH", "pass")))
        }
      ),
      suite("Bool")(
        testM("true") {
          for {
            result <- Task(BoolInput.encode(true))
          } yield assert(result)(equalTo(respArgs("1")))
        },
        testM("false") {
          for {
            result <- Task(BoolInput.encode(false))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("Stralgocommand")(
        test("length option") {
          assert(StralgoLcsQueryTypeInput.encode(StrAlgoLcsQueryType.Len))(
            equalTo(respArgs("LEN"))
          )
        },
        test("idx option default") {
          assert(StralgoLcsQueryTypeInput.encode(Idx()))(
            equalTo(respArgs("IDX"))
          )
        },
        test("idx option with minmatchlength") {
          assert(StralgoLcsQueryTypeInput.encode(Idx(minMatchLength = 2)))(
            equalTo(respArgs("IDX", "MINMATCHLEN", "2"))
          )
        },
        test("idx option with withmatchlength") {
          assert(StralgoLcsQueryTypeInput.encode(Idx(withMatchLength = true)))(
            equalTo(respArgs("IDX", "WITHMATCHLEN"))
          )
        },
        test("idx option with minmatchlength and withmatchlength") {
          assert(StralgoLcsQueryTypeInput.encode(Idx(minMatchLength = 2, withMatchLength = true)))(
            equalTo(respArgs("IDX", "MINMATCHLEN", "2", "WITHMATCHLEN"))
          )
        }
      ),
      suite("BitFieldCommand")(
        testM("get with unsigned type and positive offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 2)))
          } yield assert(result)(equalTo(respArgs("GET", "u3", "2")))
        },
        testM("get with signed type and negative offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(SignedInt(3), -2)))
          } yield assert(result)(equalTo(respArgs("GET", "i3", "-2")))
        },
        testM("get with unsigned type and zero offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 0)))
          } yield assert(result)(equalTo(respArgs("GET", "u3", "0")))
        },
        testM("set with unsigned type, positive offset and positive value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(respArgs("SET", "u3", "2", "100")))
        },
        testM("set with signed type, negative offset and negative value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(respArgs("SET", "i3", "-2", "-100")))
        },
        testM("set with unsigned type, zero offset and zero value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(respArgs("SET", "u3", "0", "0")))
        },
        testM("incr with unsigned type, positive offset and positive value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "u3", "2", "100")))
        },
        testM("incr with signed type, negative offset and negative value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "i3", "-2", "-100")))
        },
        testM("incr with unsigned type, zero offset and zero value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "u3", "0", "0")))
        },
        testM("overflow sat") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Sat))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "SAT")))
        },
        testM("overflow fail") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Fail))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "FAIL")))
        },
        testM("overflow warp") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Wrap))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "WRAP")))
        }
      ),
      suite("BitOperation")(
        testM("and") {
          for {
            result <- Task(BitOperationInput.encode(AND))
          } yield assert(result)(equalTo(respArgs("AND")))
        },
        testM("or") {
          for {
            result <- Task(BitOperationInput.encode(OR))
          } yield assert(result)(equalTo(respArgs("OR")))
        },
        testM("xor") {
          for {
            result <- Task(BitOperationInput.encode(XOR))
          } yield assert(result)(equalTo(respArgs("XOR")))
        },
        testM("not") {
          for {
            result <- Task(BitOperationInput.encode(NOT))
          } yield assert(result)(equalTo(respArgs("NOT")))
        }
      ),
      suite("BitPosRange")(
        testM("with only start") {
          for {
            result <- Task(BitPosRangeInput.encode(BitPosRange(1.second.toMillis, None)))
          } yield assert(result)(equalTo(respArgs("1000")))
        },
        testM("with start and the end") {
          for {
            result <- Task(BitPosRangeInput.encode(BitPosRange(0.second.toMillis, Some(1.second.toMillis))))
          } yield assert(result)(equalTo(respArgs("0", "1000")))
        }
      ),
      suite("By")(
        testM("with a pattern") {
          for {
            result <- Task(ByInput.encode("mykey_*"))
          } yield assert(result)(equalTo(respArgs("BY", "mykey_*")))
        }
      ),
      suite("Changed")(
        testM("valid value") {
          for {
            result <- Task(ChangedInput.encode(Changed))
          } yield assert(result)(equalTo(respArgs("CH")))
        }
      ),
      suite("ClientKill")(
        testM("address") {
          val address = InetAddress.getByName("127.0.0.1")
          val port    = 42
          for {
            result <- Task(ClientKillInput.encode(ClientKillFilter.Address(address, port)))
          } yield assert(result)(equalTo(respArgs("ADDR", "127.0.0.1:42")))
        },
        testM("local address") {
          val address = InetAddress.getByName("127.0.0.1")
          val port    = 42
          for {
            result <- Task(ClientKillInput.encode(ClientKillFilter.LocalAddress(address, port)))
          } yield assert(result)(equalTo(respArgs("LADDR", s"127.0.0.1:42")))
        },
        testM("client id") {
          val id = 42L
          for {
            result <- Task(ClientKillInput.encode(ClientKillFilter.Id(id)))
          } yield assert(result)(equalTo(respArgs("ID", "42")))
        },
        testM("type") {
          val clientType = ClientType.PubSub
          for {
            result <- Task(ClientKillInput.encode(ClientKillFilter.Type(clientType)))
          } yield assert(result)(equalTo(respArgs("TYPE", "pubsub")))
        },
        testM("user") {
          val user = "Foo Bar"
          for {
            result <- Task(ClientKillInput.encode(ClientKillFilter.User(user)))
          } yield assert(result)(equalTo(respArgs("USER", "Foo Bar")))
        },
        testM("skip me") {
          for {
            result <- Task(ClientKillInput.encode(ClientKillFilter.SkipMe(true)))
          } yield assert(result)(equalTo(respArgs("SKIPME", "YES")))
        }
      ),
      suite("ClientType")(
        testM("valid value") {
          for {
            result <- Task(ClientTypeInput.encode(ClientType.Replica))
          } yield assert(result)(equalTo(respArgs("TYPE", "replica")))
        }
      ),
      suite("ClientPauseMode")(
        testM("all") {
          for {
            result <- Task(ClientPauseModeInput.encode(ClientPauseMode.All))
          } yield assert(result)(equalTo(respArgs("ALL")))
        },
        testM("write") {
          for {
            result <- Task(ClientPauseModeInput.encode(ClientPauseMode.Write))
          } yield assert(result)(equalTo(respArgs("WRITE")))
        }
      ),
      suite("ClientTracking")(
        testM("off") {
          for {
            result <- Task(ClientTrackingInput.encode(None))
          } yield assert(result)(equalTo(respArgs("OFF")))
        },
        testM("client redirect with noloop and prefixes") {
          val clientId = 42L
          val prefixes = Chunk("prefix1", "prefix2", "prefix3")
          for {
            result <- Task(ClientTrackingInput.encode(Some((Some(clientId), None, true, prefixes))))
          } yield assert(result)(
            equalTo(
              respArgs("ON", "REDIRECT", clientId.toString) ++ prefixes
                .flatMap(respArgs("PREFIX", _)) ++ respArgs("NOLOOP")
            )
          )
        },
        testM("broadcast mode") {
          for {
            result <-
              Task(ClientTrackingInput.encode(Some((None, Some(ClientTrackingMode.Broadcast), false, Chunk.empty))))
          } yield assert(result)(equalTo(respArgs("ON", "BCAST")))
        }
      ),
      suite("Copy")(
        testM("valid value") {
          for {
            result <- Task(CopyInput.encode(Copy))
          } yield assert(result)(equalTo(respArgs("COPY")))
        }
      ),
      suite("Count")(
        testM("positive value") {
          for {
            result <- Task(CountInput.encode(Count(3L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "3")))
        },
        testM("negative value") {
          for {
            result <- Task(CountInput.encode(Count(-3L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "-3")))
        },
        testM("zero value") {
          for {
            result <- Task(CountInput.encode(Count(0L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "0")))
        }
      ),
      suite("Position")(
        testM("before") {
          for {
            result <- Task(PositionInput.encode(Position.Before))
          } yield assert(result)(equalTo(respArgs("BEFORE")))
        },
        testM("after") {
          for {
            result <- Task(PositionInput.encode(Position.After))
          } yield assert(result)(equalTo(respArgs("AFTER")))
        }
      ),
      suite("RedisType")(
        testM("string type") {
          for {
            result <- Task(RedisTypeInput.encode(RedisType.String))
          } yield assert(result)(equalTo(respArgs("TYPE", "string")))
        },
        testM("list type") {
          for {
            result <- Task(RedisTypeInput.encode(RedisType.List))
          } yield assert(result)(equalTo(respArgs("TYPE", "list")))
        },
        testM("set type") {
          for {
            result <- Task(RedisTypeInput.encode(RedisType.Set))
          } yield assert(result)(equalTo(respArgs("TYPE", "set")))
        },
        testM("sorted set type") {
          for {
            result <- Task(RedisTypeInput.encode(RedisType.SortedSet))
          } yield assert(result)(equalTo(respArgs("TYPE", "zset")))
        },
        testM("hash type") {
          for {
            result <- Task(RedisTypeInput.encode(RedisType.Hash))
          } yield assert(result)(equalTo(respArgs("TYPE", "hash")))
        },
        testM("stream type") {
          for {
            result <- Task(RedisTypeInput.encode(RedisType.Stream))
          } yield assert(result)(equalTo(respArgs("TYPE", "stream")))
        }
      ),
      suite("Double")(
        testM("positive value") {
          for {
            result <- Task(DoubleInput.encode(4.2d))
          } yield assert(result)(equalTo(respArgs("4.2")))
        },
        testM("negative value") {
          for {
            result <- Task(DoubleInput.encode(-4.2d))
          } yield assert(result)(equalTo(respArgs("-4.2")))
        },
        testM("zero value") {
          for {
            result <- Task(DoubleInput.encode(0d))
          } yield assert(result)(equalTo(respArgs("0.0")))
        }
      ),
      suite("DurationMilliseconds")(
        testM("1 second") {
          for {
            result <- Task(DurationMillisecondsInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("1000")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationMillisecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("100")))
        }
      ),
      suite("DurationSeconds")(
        testM("1 minute") {
          for {
            result <- Task(DurationSecondsInput.encode(1.minute))
          } yield assert(result)(equalTo(respArgs("60")))
        },
        testM("1 second") {
          for {
            result <- Task(DurationSecondsInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("1")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationSecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("DurationTtl")(
        testM("1 second") {
          for {
            result <- Task(DurationTtlInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("PX", "1000")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationTtlInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("PX", "100")))
        }
      ),
      suite("Freq")(
        testM("empty string") {
          for {
            result <- Task(FreqInput.encode(Freq("")))
          } yield assert(result)(equalTo(respArgs("FREQ", "")))
        },
        testM("non-empty string") {
          for {
            result <- Task(FreqInput.encode(Freq("frequency")))
          } yield assert(result)(equalTo(respArgs("FREQ", "frequency")))
        }
      ),
      suite("Get")(
        testM("with a pattern") {
          for {
            result <- Task(GetInput.encode("mypattern_*"))
          } yield assert(result)(equalTo(respArgs("GET", "mypattern_*")))
        }
      ),
      suite("IdleTime")(
        testM("0 seconds") {
          for {
            result <- Task(IdleTimeInput.encode(IdleTime(0)))
          } yield assert(result)(equalTo(respArgs("IDLETIME", "0")))
        },
        testM("5 seconds") {
          for {
            result <- Task(IdleTimeInput.encode(IdleTime(5)))
          } yield assert(result)(equalTo(respArgs("IDLETIME", "5")))
        }
      ),
      suite("Increment")(
        testM("valid value") {
          for {
            result <- Task(IncrementInput.encode(Increment))
          } yield assert(result)(equalTo(respArgs("INCR")))
        }
      ),
      suite("KeepTtl")(
        testM("valid value") {
          for {
            result <- Task(KeepTtlInput.encode(KeepTtl))
          } yield assert(result)(equalTo(respArgs("KEEPTTL")))
        }
      ),
      suite("LexRange")(
        testM("with unbound min and unbound max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Unbounded.stringify, LexMaximum.Unbounded.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-", "+")))
        },
        testM("with open min and unbound max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Open("a").stringify, LexMaximum.Unbounded.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(a", "+")))
        },
        testM("with closed min and unbound max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Closed("a").stringify, LexMaximum.Unbounded.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("[a", "+")))
        },
        testM("with unbound min and open max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Unbounded.stringify, LexMaximum.Open("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-", "(z")))
        },
        testM("with open min and open max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Open("a").stringify, LexMaximum.Open("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(a", "(z")))
        },
        testM("with closed min and open max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Closed("a").stringify, LexMaximum.Open("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("[a", "(z")))
        },
        testM("with unbound min and closed max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Unbounded.stringify, LexMaximum.Closed("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-", "[z")))
        },
        testM("with open min and closed max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Open("a").stringify, LexMaximum.Closed("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(a", "[z")))
        },
        testM("with closed min and closed max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Closed("a").stringify, LexMaximum.Closed("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("[a", "[z")))
        }
      ),
      suite("Limit")(
        testM("with positive offset and positive count") {
          for {
            result <- Task(LimitInput.encode(Limit(4L, 5L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "4", "5")))
        },
        testM("with negative offset and negative count") {
          for {
            result <- Task(LimitInput.encode(Limit(-4L, -5L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "-4", "-5")))
        },
        testM("with zero offset and zero count") {
          for {
            result <- Task(LimitInput.encode(Limit(0L, 0L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "0", "0")))
        }
      ),
      suite("Long")(
        testM("positive value") {
          for {
            result <- Task(LongInput.encode(4L))
          } yield assert(result)(equalTo(respArgs("4")))
        },
        testM("negative value") {
          for {
            result <- Task(LongInput.encode(-4L))
          } yield assert(result)(equalTo(respArgs("-4")))
        },
        testM("zero value") {
          for {
            result <- Task(LongInput.encode(0L))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("LongLat")(
        testM("positive longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(4.2d, 5.2d)))
          } yield assert(result)(equalTo(respArgs("4.2", "5.2")))
        },
        testM("negative longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(-4.2d, -5.2d)))
          } yield assert(result)(equalTo(respArgs("-4.2", "-5.2")))
        },
        testM("zero longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(0d, 0d)))
          } yield assert(result)(equalTo(respArgs("0.0", "0.0")))
        }
      ),
      suite("MemberScore")(
        testM("with positive score and empty member") {
          for {
            result <- Task(MemberScoreInput[String]().encode(MemberScore(4.2d, "")))
          } yield assert(result)(equalTo(respArgs("4.2", "")))
        },
        testM("with negative score and empty member") {
          for {
            result <- Task(MemberScoreInput[String]().encode(MemberScore(-4.2d, "")))
          } yield assert(result)(equalTo(respArgs("-4.2", "")))
        },
        testM("with zero score and empty member") {
          for {
            result <- Task(MemberScoreInput[String]().encode(MemberScore(0d, "")))
          } yield assert(result)(equalTo(respArgs("0.0", "")))
        },
        testM("with positive score and non-empty member") {
          for {
            result <- Task(MemberScoreInput[String]().encode(MemberScore(4.2d, "member")))
          } yield assert(result)(equalTo(respArgs("4.2", "member")))
        },
        testM("with negative score and non-empty member") {
          for {
            result <- Task(MemberScoreInput[String]().encode(MemberScore(-4.2d, "member")))
          } yield assert(result)(equalTo(respArgs("-4.2", "member")))
        },
        testM("with zero score and non-empty member") {
          for {
            result <- Task(MemberScoreInput[String]().encode(MemberScore(0d, "member")))
          } yield assert(result)(equalTo(respArgs("0.0", "member")))
        }
      ),
      suite("NoInput")(
        testM("valid value") {
          for {
            result <- Task(NoInput.encode(()))
          } yield assert(result)(isEmpty)
        }
      ),
      suite("NonEmptyList")(
        testM("with multiple elements") {
          for {
            result <- Task(NonEmptyList(StringInput).encode(("a", List("b", "c"))))
          } yield assert(result)(equalTo(respArgs("a", "b", "c")))
        },
        testM("with one element") {
          for {
            result <- Task(NonEmptyList(StringInput).encode(("a", List.empty)))
          } yield assert(result)(equalTo(respArgs("a")))
        }
      ),
      suite("Order")(
        testM("ascending") {
          for {
            result <- Task(OrderInput.encode(Ascending))
          } yield assert(result)(equalTo(respArgs("ASC")))
        },
        testM("descending") {
          for {
            result <- Task(OrderInput.encode(Descending))
          } yield assert(result)(equalTo(respArgs("DESC")))
        }
      ),
      suite("RadiusUnit")(
        testM("meters") {
          for {
            result <- Task(RadiusUnitInput.encode(Meters))
          } yield assert(result)(equalTo(respArgs("m")))
        },
        testM("kilometers") {
          for {
            result <- Task(RadiusUnitInput.encode(Kilometers))
          } yield assert(result)(equalTo(respArgs("km")))
        },
        testM("feet") {
          for {
            result <- Task(RadiusUnitInput.encode(Feet))
          } yield assert(result)(equalTo(respArgs("ft")))
        },
        testM("miles") {
          for {
            result <- Task(RadiusUnitInput.encode(Miles))
          } yield assert(result)(equalTo(respArgs("mi")))
        }
      ),
      suite("Range")(
        testM("with positive start and positive end") {
          for {
            result <- Task(RangeInput.encode(Range(1, 5)))
          } yield assert(result)(equalTo(respArgs("1", "5")))
        },
        testM("with negative start and positive end") {
          for {
            result <- Task(RangeInput.encode(Range(-1, 5)))
          } yield assert(result)(equalTo(respArgs("-1", "5")))
        },
        testM("with positive start and negative end") {
          for {
            result <- Task(RangeInput.encode(Range(1, -5)))
          } yield assert(result)(equalTo(respArgs("1", "-5")))
        },
        testM("with negative start and negative end") {
          for {
            result <- Task(RangeInput.encode(Range(-1, -5)))
          } yield assert(result)(equalTo(respArgs("-1", "-5")))
        }
      ),
      suite("Pattern")(
        testM("with valid pattern") {
          for {
            result <- Task(PatternInput.encode(Pattern("*[ab]-*")))
          } yield assert(result)(equalTo(respArgs("MATCH", "*[ab]-*")))
        },
        testM("with empty pattern") {
          for {
            result <- Task(PatternInput.encode(Pattern("")))
          } yield assert(result)(equalTo(respArgs("MATCH", "")))
        }
      ),
      suite("Replace")(
        testM("valid value") {
          for {
            result <- Task(ReplaceInput.encode(Replace))
          } yield assert(result)(equalTo(respArgs("REPLACE")))
        }
      ),
      suite("StoreDist")(
        testM("with non-empty string") {
          for {
            result <- Task(StoreDistInput.encode(StoreDist("key")))
          } yield assert(result)(equalTo(respArgs("STOREDIST", "key")))
        },
        testM("with empty string") {
          for {
            result <- Task(StoreDistInput.encode(StoreDist("")))
          } yield assert(result)(equalTo(respArgs("STOREDIST", "")))
        }
      ),
      suite("Store")(
        testM("with non-empty string") {
          for {
            result <- Task(StoreInput.encode(Store("key")))
          } yield assert(result)(equalTo(respArgs("STORE", "key")))
        },
        testM("with empty string") {
          for {
            result <- Task(StoreInput.encode(Store("")))
          } yield assert(result)(equalTo(respArgs("STORE", "")))
        }
      ),
      suite("ScoreRange")(
        testM("with infinite min and infinite max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Infinity.stringify, ScoreMaximum.Infinity.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-inf", "+inf")))
        },
        testM("with open min and infinite max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).stringify, ScoreMaximum.Infinity.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(4.2", "+inf")))
        },
        testM("with closed min and infinite max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).stringify, ScoreMaximum.Infinity.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("4.2", "+inf")))
        },
        testM("with infinite min and open max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Infinity.stringify, ScoreMaximum.Open(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-inf", "(5.2")))
        },
        testM("with open min and open max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).stringify, ScoreMaximum.Open(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(4.2", "(5.2")))
        },
        testM("with closed min and open max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).stringify, ScoreMaximum.Open(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("4.2", "(5.2")))
        },
        testM("with infinite min and closed max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Infinity.stringify, ScoreMaximum.Closed(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-inf", "5.2")))
        },
        testM("with open min and closed max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).stringify, ScoreMaximum.Closed(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(4.2", "5.2")))
        },
        testM("with closed min and closed max") {
          for {
            result <- Task(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).stringify, ScoreMaximum.Closed(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("4.2", "5.2")))
        }
      ),
      suite("String")(
        testM("non-empty value") {
          for {
            result <- Task(StringInput.encode("non-empty"))
          } yield assert(result)(equalTo(respArgs("non-empty")))
        },
        testM("empty value") {
          for {
            result <- Task(StringInput.encode(""))
          } yield assert(result)(equalTo(respArgs("")))
        }
      ),
      suite("Optional")(
        testM("none") {
          for {
            result <- Task(OptionalInput(LongInput).encode(None))
          } yield assert(result)(isEmpty)
        },
        testM("some") {
          for {
            result <- Task(OptionalInput(LongInput).encode(Some(2L)))
          } yield assert(result)(equalTo(respArgs("2")))
        }
      ),
      suite("TimeSeconds")(
        testM("positiv value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(respArgs("3")))
        },
        testM("zero value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(respArgs("0")))
        },
        testM("negative value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(respArgs("-3")))
        }
      ),
      suite("TimeMilliseconds")(
        testM("positiv value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(respArgs("3000")))
        },
        testM("zero value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(respArgs("0")))
        },
        testM("negative value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(respArgs("-3000")))
        }
      ),
      suite("Tuple2")(
        testM("valid value") {
          for {
            result <- Task(Tuple2(StringInput, LongInput).encode(("one", 2L)))
          } yield assert(result)(equalTo(respArgs("one", "2")))
        }
      ),
      suite("Tuple3")(
        testM("valid value") {
          for {
            result <- Task(Tuple3(StringInput, LongInput, StringInput).encode(("one", 2, "three")))
          } yield assert(result)(equalTo(respArgs("one", "2", "three")))
        }
      ),
      suite("Tuple4")(
        testM("valid value") {
          for {
            result <- Task(Tuple4(StringInput, LongInput, StringInput, LongInput).encode(("one", 2, "three", 4)))
          } yield assert(result)(equalTo(respArgs("one", "2", "three", "4")))
        }
      ),
      suite("Tuple5")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple5(StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five"))
                      )
          } yield assert(result)(
            equalTo(respArgs("one", "2", "three", "4", "five"))
          )
        }
      ),
      suite("Tuple7")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple7(StringInput, LongInput, StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five", 6, "seven"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven"
              )
            )
          )
        }
      ),
      suite("Tuple9")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple9(
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput
                        ).encode(("one", 2, "three", 4, "five", 6, "seven", 8, "nine"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven",
                "8",
                "nine"
              )
            )
          )
        }
      ),
      suite("Tuple11")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple11(
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput
                        ).encode(("one", 2, "three", 4, "five", 6, "seven", 8, "nine", 10, "eleven"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven",
                "8",
                "nine",
                "10",
                "eleven"
              )
            )
          )
        }
      ),
      suite("Update")(
        testM("set existing") {
          for {
            result <- Task(UpdateInput.encode(Update.SetExisting))
          } yield assert(result)(equalTo(respArgs("XX")))
        },
        testM("set new") {
          for {
            result <- Task(UpdateInput.encode(Update.SetNew))
          } yield assert(result)(equalTo(respArgs("NX")))
        }
      ),
      suite("Id")(
        testM("valid value") {
          for {
            result <- Task(IdInput.encode(10))
          } yield assert(result)(equalTo(respArgs("ID", "10")))
        }
      ),
      suite("UnblockBehavior")(
        testM("timeout") {
          for {
            result <- Task(UnblockBehaviorInput.encode(UnblockBehavior.Timeout))
          } yield assert(result)(equalTo(respArgs("TIMEOUT")))
        },
        testM("error") {
          for {
            result <- Task(UnblockBehaviorInput.encode(UnblockBehavior.Error))
          } yield assert(result)(equalTo(respArgs("ERROR")))
        }
      ),
      suite("Varargs")(
        testM("with multiple elements") {
          for {
            result <- Task(Varargs(LongInput).encode(List(1, 2, 3)))
          } yield assert(result)(equalTo(respArgs("1", "2", "3")))
        },
        testM("with no elements") {
          for {
            result <- Task(Varargs(LongInput).encode(List.empty))
          } yield assert(result)(isEmpty)
        }
      ),
      suite("WithScores")(
        testM("valid value") {
          for {
            result <- Task(WithScoresInput.encode(WithScores))
          } yield assert(result)(equalTo(respArgs("WITHSCORES")))
        }
      ),
      suite("WithCoord")(
        testM("valid value") {
          for {
            result <- Task(WithCoordInput.encode(WithCoord))
          } yield assert(result)(equalTo(respArgs("WITHCOORD")))
        }
      ),
      suite("WithDist")(
        testM("valid value") {
          for {
            result <- Task(WithDistInput.encode(WithDist))
          } yield assert(result)(equalTo(respArgs("WITHDIST")))
        }
      ),
      suite("WithHash")(
        testM("valid value") {
          for {
            result <- Task(WithHashInput.encode(WithHash))
          } yield assert(result)(equalTo(respArgs("WITHHASH")))
        }
      ),
      suite("Idle")(
        testM("with 1 second") {
          Task(IdleInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("IDLE", "1000"))))
        },
        testM("with 100 milliseconds") {
          Task(IdleInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("IDLE", "100"))))
        },
        testM("with negative duration") {
          Task(IdleInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("IDLE", "0"))))
        }
      ),
      suite("Time")(
        testM("with 1 second") {
          Task(TimeInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("TIME", "1000"))))
        },
        testM("with 100 milliseconds") {
          Task(TimeInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("TIME", "100"))))
        },
        testM("with negative duration") {
          Task(TimeInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("TIME", "0"))))
        }
      ),
      suite("RetryCount")(
        testM("with positive count") {
          Task(RetryCountInput.encode(100))
            .map(assert(_)(equalTo(respArgs("RETRYCOUNT", "100"))))
        },
        testM("with negative count") {
          Task(RetryCountInput.encode(-100))
            .map(assert(_)(equalTo(respArgs("RETRYCOUNT", "-100"))))
        }
      ),
      suite("XGroupCreate")(
        testM("without mkStream") {
          Task(
            XGroupCreateInput[String, String, String]().encode(
              XGroupCommand.Create("key", "group", "id", mkStream = false)
            )
          )
            .map(assert(_)(equalTo(respArgs("CREATE", "key", "group", "id"))))
        },
        testM("with mkStream") {
          Task(
            XGroupCreateInput[String, String, String]().encode(
              XGroupCommand.Create("key", "group", "id", mkStream = true)
            )
          )
            .map(assert(_)(equalTo(respArgs("CREATE", "key", "group", "id", "MKSTREAM"))))
        }
      ),
      suite("XGroupSetId")(
        testM("valid value") {
          Task(XGroupSetIdInput[String, String, String]().encode(XGroupCommand.SetId("key", "group", "id")))
            .map(assert(_)(equalTo(respArgs("SETID", "key", "group", "id"))))
        }
      ),
      suite("XGroupDestroy")(
        testM("valid value") {
          Task(XGroupDestroyInput[String, String]().encode(XGroupCommand.Destroy("key", "group")))
            .map(assert(_)(equalTo(respArgs("DESTROY", "key", "group"))))
        }
      ),
      suite("XGroupCreateConsumer")(
        testM("valid value") {
          Task(
            XGroupCreateConsumerInput[String, String, String]().encode(
              XGroupCommand.CreateConsumer("key", "group", "consumer")
            )
          )
            .map(assert(_)(equalTo(respArgs("CREATECONSUMER", "key", "group", "consumer"))))
        }
      ),
      suite("XGroupDelConsumer")(
        testM("valid value") {
          Task(
            XGroupDelConsumerInput[String, String, String]().encode(
              XGroupCommand.DelConsumer("key", "group", "consumer")
            )
          )
            .map(assert(_)(equalTo(respArgs("DELCONSUMER", "key", "group", "consumer"))))
        }
      ),
      suite("Block")(
        testM("with 1 second") {
          Task(BlockInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("BLOCK", "1000"))))
        },
        testM("with 100 milliseconds") {
          Task(BlockInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("BLOCK", "100"))))
        },
        testM("with negative duration") {
          Task(BlockInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("BLOCK", "0"))))
        }
      ),
      suite("Streams")(
        testM("with one pair") {
          Task(StreamsInput[String, String]().encode(("a" -> "b", Chunk.empty)))
            .map(assert(_)(equalTo(respArgs("STREAMS", "a", "b"))))
        },
        testM("with multiple pairs") {
          Task(StreamsInput[String, String]().encode(("a" -> "b", Chunk.single("c" -> "d"))))
            .map(assert(_)(equalTo(respArgs("STREAMS", "a", "c", "b", "d"))))
        }
      ),
      suite("NoAck")(
        testM("valid value") {
          Task(NoAckInput.encode(NoAck)).map(assert(_)(equalTo(respArgs("NOACK"))))
        }
      ),
      suite("MaxLen")(
        testM("with approximate") {
          Task(StreamMaxLenInput.encode(StreamMaxLen(approximate = true, 10)))
            .map(assert(_)(equalTo(respArgs("MAXLEN", "~", "10"))))
        },
        testM("without approximate") {
          Task(StreamMaxLenInput.encode(StreamMaxLen(approximate = false, 10)))
            .map(assert(_)(equalTo(respArgs("MAXLEN", "10"))))
        }
      ),
      suite("WithForce")(
        testM("valid value") {
          Task(WithForceInput.encode(WithForce)).map(assert(_)(equalTo(respArgs("FORCE"))))
        }
      ),
      suite("WithJustId")(
        testM("valid value") {
          Task(WithJustIdInput.encode(WithJustId)).map(assert(_)(equalTo(respArgs("JUSTID"))))
        }
      ),
      suite("Side")(
        testM("left") {
          for {
            result <- Task(SideInput.encode(Side.Left))
          } yield assert(result)(equalTo(respArgs("LEFT")))
        },
        testM("right") {
          for {
            result <- Task(SideInput.encode(Side.Right))
          } yield assert(result)(equalTo(respArgs("RIGHT")))
        }
      ),
      suite("ListMaxLen")(
        testM("valid value") {
          Task(ListMaxLenInput.encode(ListMaxLen(10L))).map(assert(_)(equalTo(respArgs("MAXLEN", "10"))))
        }
      ),
      suite("Rank")(
        testM("valid value") {
          Task(RankInput.encode(Rank(10L))).map(assert(_)(equalTo(respArgs("RANK", "10"))))
        }
      ),
      suite("GetEx")(
        testM("GetExInput - valid value") {
          for {
            resultSeconds <-
              Task(GetExInput[String]().encode(scala.Tuple3.apply("key", Expire.SetExpireSeconds, 1.second)))
            resultMilliseconds <-
              Task(GetExInput[String]().encode(scala.Tuple3("key", Expire.SetExpireMilliseconds, 100.millis)))
          } yield assert(resultSeconds)(equalTo(respArgs("key", "EX", "1"))) && assert(resultMilliseconds)(
            equalTo(respArgs("key", "PX", "100"))
          )
        },
        testM("GetExAtInput - valid value") {
          for {
            resultSeconds <-
              Task(
                GetExAtInput[String]().encode(
                  scala.Tuple3("key", ExpiredAt.SetExpireAtSeconds, Instant.parse("2021-04-06T00:00:00Z"))
                )
              )
            resultMilliseconds <-
              Task(
                GetExAtInput[String]().encode(
                  scala.Tuple3("key", ExpiredAt.SetExpireAtMilliseconds, Instant.parse("2021-04-06T00:00:00Z"))
                )
              )
          } yield assert(resultSeconds)(equalTo(respArgs("key", "EXAT", "1617667200"))) && assert(resultMilliseconds)(
            equalTo(respArgs("key", "PXAT", "1617667200000"))
          )
        },
        testM("GetExPersistInput - valid value") {
          for {
            result              <- Task(GetExPersistInput[String]().encode("key" -> true))
            resultWithoutOption <- Task(GetExPersistInput[String]().encode("key" -> false))
          } yield assert(result)(equalTo(respArgs("key", "PERSIST"))) &&
            assert(resultWithoutOption)(equalTo(respArgs("key")))
        }
      ),
      suite("YesNo")(
        testM("yes") {
          for {
            result <- Task(YesNoInput.encode(true))
          } yield assert(result)(equalTo(respArgs("YES")))
        },
        testM("no") {
          for {
            result <- Task(YesNoInput.encode(false))
          } yield assert(result)(equalTo(respArgs("NO")))
        }
      )
    )

  private def respArgs(xs: String*): Chunk[RespValue] = Chunk.fromIterable(xs.map(RespValue.bulkString))
}
