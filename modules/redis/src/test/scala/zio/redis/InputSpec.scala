package zio.redis

import zio._
import zio.prelude.Newtype.unsafeWrap
import zio.redis.Input._
import zio.redis.internal.RespCommand
import zio.redis.internal.RespCommandArgument._
import zio.redis.options.PositiveLong
import zio.test.Assertion._
import zio.test._

import java.net.InetAddress
import java.time.Instant

object InputSpec extends BaseSpec {
  import BitFieldCommand._
  import BitFieldType._
  import BitOperation._
  import LcsQueryType._
  import Order._
  import RadiusUnit._

  def spec: Spec[Any, Throwable] =
    suite("Input encoders")(
      suite("AbsTtl")(
        test("valid value") {
          for {
            result <- ZIO.attempt(AbsTtlInput.encode(AbsTtl))
          } yield assert(result)(equalTo(RespCommand(Literal("ABSTTL"))))
        }
      ),
      suite("Address")(
        test("valid value") {
          for {
            ip     <- ZIO.succeed(InetAddress.getByName("127.0.0.1"))
            port   <- ZIO.succeed(42)
            result <- ZIO.attempt(AddressInput.encode(Address(ip, port)))
          } yield assert(result)(equalTo(RespCommand(Value("127.0.0.1:42"))))
        }
      ),
      suite("Aggregate")(
        test("max") {
          for {
            result <- ZIO.attempt(AggregateInput.encode(Aggregate.Max))
          } yield assert(result)(equalTo(RespCommand(Literal("AGGREGATE"), Literal("MAX"))))
        },
        test("min") {
          for {
            result <- ZIO.attempt(AggregateInput.encode(Aggregate.Min))
          } yield assert(result)(equalTo(RespCommand(Literal("AGGREGATE"), Literal("MIN"))))
        },
        test("sum") {
          for {
            result <- ZIO.attempt(AggregateInput.encode(Aggregate.Sum))
          } yield assert(result)(equalTo(RespCommand(Literal("AGGREGATE"), Literal("SUM"))))
        }
      ),
      suite("Alpha")(
        test("alpha") {
          for {
            result <- ZIO.attempt(AlphaInput.encode(Alpha))
          } yield assert(result)(equalTo(RespCommand(Literal("ALPHA"))))
        }
      ),
      suite("Auth")(
        test("with empty password") {
          for {
            result <- ZIO.attempt(AuthInput.encode(Auth(None, "")))
          } yield assert(result)(equalTo(RespCommand(Value(""))))
        },
        test("with non-empty password") {
          for {
            result <- ZIO.attempt(AuthInput.encode(Auth(None, "pass")))
          } yield assert(result)(equalTo(RespCommand(Value("pass"))))
        },
        test("with both username and password") {
          for {
            result <- ZIO.attempt(AuthInput.encode(Auth(Some("user"), "pass")))
          } yield assert(result)(equalTo(RespCommand(Value("user"), Value("pass"))))
        }
      ),
      suite("Bool")(
        test("true") {
          for {
            result <- ZIO.attempt(BoolInput.encode(true))
          } yield assert(result)(equalTo(RespCommand(Literal("1"))))
        },
        test("false") {
          for {
            result <- ZIO.attempt(BoolInput.encode(false))
          } yield assert(result)(equalTo(RespCommand(Literal("0"))))
        }
      ),
      suite("LcsQueryType")(
        test("length option") {
          assert(LcsQueryTypeInput.encode(LcsQueryType.Len))(
            equalTo(RespCommand(Literal("LEN")))
          )
        },
        test("idx option default") {
          assert(LcsQueryTypeInput.encode(Idx()))(
            equalTo(RespCommand(Literal("IDX")))
          )
        },
        test("idx option with minmatchlength") {
          assert(LcsQueryTypeInput.encode(Idx(minMatchLength = 2)))(
            equalTo(RespCommand(Literal("IDX"), Literal("MINMATCHLEN"), Value("2")))
          )
        },
        test("idx option with withmatchlength") {
          assert(LcsQueryTypeInput.encode(Idx(withMatchLength = true)))(
            equalTo(RespCommand(Literal("IDX"), Literal("WITHMATCHLEN")))
          )
        },
        test("idx option with minmatchlength and withmatchlength") {
          assert(LcsQueryTypeInput.encode(Idx(minMatchLength = 2, withMatchLength = true)))(
            equalTo(RespCommand(Literal("IDX"), Literal("MINMATCHLEN"), Value("2"), Literal("WITHMATCHLEN")))
          )
        }
      ),
      suite("BitFieldCommand")(
        test("get with unsigned type and positive offset") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 2)))
          } yield assert(result)(equalTo(RespCommand(Literal("GET"), Value("u3"), Value("2"))))
        },
        test("get with signed type and negative offset") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldGet(SignedInt(3), -2)))
          } yield assert(result)(equalTo(RespCommand(Literal("GET"), Value("i3"), Value("-2"))))
        },
        test("get with unsigned type and zero offset") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 0)))
          } yield assert(result)(equalTo(RespCommand(Literal("GET"), Value("u3"), Value("0"))))
        },
        test("set with unsigned type, positive offset and positive value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(RespCommand(Literal("SET"), Value("u3"), Value("2"), Value("100"))))
        },
        test("set with signed type, negative offset and negative value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldSet(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(RespCommand(Literal("SET"), Value("i3"), Value("-2"), Value("-100"))))
        },
        test("set with unsigned type, zero offset and zero value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(RespCommand(Literal("SET"), Value("u3"), Value("0"), Value("0"))))
        },
        test("incr with unsigned type, positive offset and positive value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(RespCommand(Literal("INCRBY"), Value("u3"), Value("2"), Value("100"))))
        },
        test("incr with signed type, negative offset and negative value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldIncr(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(RespCommand(Literal("INCRBY"), Value("i3"), Value("-2"), Value("-100"))))
        },
        test("incr with unsigned type, zero offset and zero value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(RespCommand(Literal("INCRBY"), Value("u3"), Value("0"), Value("0"))))
        },
        test("overflow sat") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldOverflow.Sat))
          } yield assert(result)(equalTo(RespCommand(Literal("OVERFLOW"), Literal("SAT"))))
        },
        test("overflow fail") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldOverflow.Fail))
          } yield assert(result)(equalTo(RespCommand(Literal("OVERFLOW"), Literal("FAIL"))))
        },
        test("overflow warp") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldOverflow.Wrap))
          } yield assert(result)(equalTo(RespCommand(Literal("OVERFLOW"), Literal("WRAP"))))
        }
      ),
      suite("BitOperation")(
        test("and") {
          for {
            result <- ZIO.attempt(BitOperationInput.encode(AND))
          } yield assert(result)(equalTo(RespCommand(Literal("AND"))))
        },
        test("or") {
          for {
            result <- ZIO.attempt(BitOperationInput.encode(OR))
          } yield assert(result)(equalTo(RespCommand(Literal("OR"))))
        },
        test("xor") {
          for {
            result <- ZIO.attempt(BitOperationInput.encode(XOR))
          } yield assert(result)(equalTo(RespCommand(Literal("XOR"))))
        },
        test("not") {
          for {
            result <- ZIO.attempt(BitOperationInput.encode(NOT))
          } yield assert(result)(equalTo(RespCommand(Literal("NOT"))))
        }
      ),
      suite("BitPosRange")(
        test("with only start") {
          for {
            result <- ZIO.attempt(BitPosRangeInput.encode(BitPosRange(1.second.toMillis, None)))
          } yield assert(result)(equalTo(RespCommand(Value("1000"))))
        },
        test("with start and the end") {
          for {
            result <- ZIO.attempt(BitPosRangeInput.encode(BitPosRange(0.second.toMillis, Some(1.second.toMillis))))
          } yield assert(result)(equalTo(RespCommand(Value("0"), Value("1000"))))
        }
      ),
      suite("By")(
        test("with a pattern") {
          for {
            result <- ZIO.attempt(ByInput.encode("mykey_*"))
          } yield assert(result)(equalTo(RespCommand(Literal("BY"), Value("mykey_*"))))
        }
      ),
      suite("Changed")(
        test("valid value") {
          for {
            result <- ZIO.attempt(ChangedInput.encode(Changed))
          } yield assert(result)(equalTo(RespCommand(Literal("CH"))))
        }
      ),
      suite("Copy")(
        test("valid value") {
          for {
            result <- ZIO.attempt(CopyInput.encode(Copy))
          } yield assert(result)(equalTo(RespCommand(Literal("COPY"))))
        }
      ),
      suite("Count")(
        test("positive value") {
          for {
            result <- ZIO.attempt(CountInput.encode(Count(3L)))
          } yield assert(result)(equalTo(RespCommand(Literal("COUNT"), Value("3"))))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(CountInput.encode(Count(-3L)))
          } yield assert(result)(equalTo(RespCommand(Literal("COUNT"), Value("-3"))))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(CountInput.encode(Count(0L)))
          } yield assert(result)(equalTo(RespCommand(Literal("COUNT"), Value("0"))))
        }
      ),
      suite("Position")(
        test("before") {
          for {
            result <- ZIO.attempt(PositionInput.encode(Position.Before))
          } yield assert(result)(equalTo(RespCommand(Literal("BEFORE"))))
        },
        test("after") {
          for {
            result <- ZIO.attempt(PositionInput.encode(Position.After))
          } yield assert(result)(equalTo(RespCommand(Literal("AFTER"))))
        }
      ),
      suite("RedisType")(
        test("string type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.String))
          } yield assert(result)(equalTo(RespCommand(Literal("TYPE"), Literal("string"))))
        },
        test("list type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.List))
          } yield assert(result)(equalTo(RespCommand(Literal("TYPE"), Literal("list"))))
        },
        test("set type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.Set))
          } yield assert(result)(equalTo(RespCommand(Literal("TYPE"), Literal("set"))))
        },
        test("sorted set type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.SortedSet))
          } yield assert(result)(equalTo(RespCommand(Literal("TYPE"), Literal("zset"))))
        },
        test("hash type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.Hash))
          } yield assert(result)(equalTo(RespCommand(Literal("TYPE"), Literal("hash"))))
        },
        test("stream type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.Stream))
          } yield assert(result)(equalTo(RespCommand(Literal("TYPE"), Literal("stream"))))
        }
      ),
      suite("Double")(
        test("positive value") {
          for {
            result <- ZIO.attempt(DoubleInput.encode(4.2d))
          } yield assert(result)(equalTo(RespCommand(Value("4.2"))))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(DoubleInput.encode(-4.2d))
          } yield assert(result)(equalTo(RespCommand(Value("-4.2"))))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(DoubleInput.encode(0d))
          } yield assert(result)(equalTo(RespCommand(Value("0.0"))))
        }
      ),
      suite("DurationMilliseconds")(
        test("1 second") {
          for {
            result <- ZIO.attempt(DurationMillisecondsInput.encode(1.second))
          } yield assert(result)(equalTo(RespCommand(Value("1000"))))
        },
        test("100 milliseconds") {
          for {
            result <- ZIO.attempt(DurationMillisecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(RespCommand(Value("100"))))
        }
      ),
      suite("DurationSeconds")(
        test("1 minute") {
          for {
            result <- ZIO.attempt(DurationSecondsInput.encode(1.minute))
          } yield assert(result)(equalTo(RespCommand(Value("60"))))
        },
        test("1 second") {
          for {
            result <- ZIO.attempt(DurationSecondsInput.encode(1.second))
          } yield assert(result)(equalTo(RespCommand(Value("1"))))
        },
        test("100 milliseconds") {
          for {
            result <- ZIO.attempt(DurationSecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(RespCommand(Value("0"))))
        }
      ),
      suite("DurationTtl")(
        test("1 second") {
          for {
            result <- ZIO.attempt(DurationTtlInput.encode(1.second))
          } yield assert(result)(equalTo(RespCommand(Literal("PX"), Value("1000"))))
        },
        test("100 milliseconds") {
          for {
            result <- ZIO.attempt(DurationTtlInput.encode(100.millis))
          } yield assert(result)(equalTo(RespCommand(Literal("PX"), Value("100"))))
        }
      ),
      suite("Freq")(
        test("empty string") {
          for {
            result <- ZIO.attempt(FreqInput.encode(Freq("")))
          } yield assert(result)(equalTo(RespCommand(Literal("FREQ"), Value(""))))
        },
        test("non-empty string") {
          for {
            result <- ZIO.attempt(FreqInput.encode(Freq("frequency")))
          } yield assert(result)(equalTo(RespCommand(Literal("FREQ"), Value("frequency"))))
        }
      ),
      suite("Get")(
        test("with a pattern") {
          for {
            result <- ZIO.attempt(GetInput.encode("mypattern_*"))
          } yield assert(result)(equalTo(RespCommand(Literal("GET"), Value("mypattern_*"))))
        }
      ),
      suite("GetKeyword")(
        test("valid value") {
          for {
            result <- ZIO.attempt(GetKeywordInput.encode(GetKeyword))
          } yield assert(result)(equalTo(RespCommand(Literal("GET"))))
        }
      ),
      suite("IdleTime")(
        test("0 seconds") {
          for {
            result <- ZIO.attempt(IdleTimeInput.encode(IdleTime(0)))
          } yield assert(result)(equalTo(RespCommand(Literal("IDLETIME"), Value("0"))))
        },
        test("5 seconds") {
          for {
            result <- ZIO.attempt(IdleTimeInput.encode(IdleTime(5)))
          } yield assert(result)(equalTo(RespCommand(Literal("IDLETIME"), Value("5"))))
        }
      ),
      suite("Increment")(
        test("valid value") {
          for {
            result <- ZIO.attempt(IncrementInput.encode(Increment))
          } yield assert(result)(equalTo(RespCommand(Literal("INCR"))))
        }
      ),
      suite("Int")(
        test("positive value") {
          for {
            result <- ZIO.attempt(IntInput.encode(4))
          } yield assert(result)(equalTo(RespCommand(Value("4"))))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(IntInput.encode(-4))
          } yield assert(result)(equalTo(RespCommand(Value("-4"))))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(IntInput.encode(0))
          } yield assert(result)(equalTo(RespCommand(Value("0"))))
        }
      ),
      suite("LexRange")(
        test("with unbound min and unbound max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Unbounded.asString, LexMaximum.Unbounded.asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("-"), Value("+"))))
        },
        test("with open min and unbound max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Open("a").asString, LexMaximum.Unbounded.asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("(a"), Value("+"))))
        },
        test("with closed min and unbound max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Closed("a").asString, LexMaximum.Unbounded.asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("[a"), Value("+"))))
        },
        test("with unbound min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Unbounded.asString, LexMaximum.Open("z").asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("-"), Value("(z"))))
        },
        test("with open min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Open("a").asString, LexMaximum.Open("z").asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("(a"), Value("(z"))))
        },
        test("with closed min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Closed("a").asString, LexMaximum.Open("z").asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("[a"), Value("(z"))))
        },
        test("with unbound min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Unbounded.asString, LexMaximum.Closed("z").asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("-"), Value("[z"))))
        },
        test("with open min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Open("a").asString, LexMaximum.Closed("z").asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("(a"), Value("[z"))))
        },
        test("with closed min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((LexMinimum.Closed("a").asString, LexMaximum.Closed("z").asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("[a"), Value("[z"))))
        }
      ),
      suite("Limit")(
        test("with positive offset and positive count") {
          for {
            result <- ZIO.attempt(LimitInput.encode(Limit(4L, 5L)))
          } yield assert(result)(equalTo(RespCommand(Literal("LIMIT"), Value("4"), Value("5"))))
        },
        test("with negative offset and negative count") {
          for {
            result <- ZIO.attempt(LimitInput.encode(Limit(-4L, -5L)))
          } yield assert(result)(equalTo(RespCommand(Literal("LIMIT"), Value("-4"), Value("-5"))))
        },
        test("with zero offset and zero count") {
          for {
            result <- ZIO.attempt(LimitInput.encode(Limit(0L, 0L)))
          } yield assert(result)(equalTo(RespCommand(Literal("LIMIT"), Value("0"), Value("0"))))
        }
      ),
      suite("Long")(
        test("positive value") {
          for {
            result <- ZIO.attempt(LongInput.encode(4L))
          } yield assert(result)(equalTo(RespCommand(Value("4"))))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(LongInput.encode(-4L))
          } yield assert(result)(equalTo(RespCommand(Value("-4"))))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(LongInput.encode(0L))
          } yield assert(result)(equalTo(RespCommand(Value("0"))))
        }
      ),
      suite("LongLat")(
        test("positive longitude and latitude") {
          for {
            result <- ZIO.attempt(LongLatInput.encode(LongLat(4.2d, 5.2d)))
          } yield assert(result)(equalTo(RespCommand(Value("4.2"), Value("5.2"))))
        },
        test("negative longitude and latitude") {
          for {
            result <- ZIO.attempt(LongLatInput.encode(LongLat(-4.2d, -5.2d)))
          } yield assert(result)(equalTo(RespCommand(Value("-4.2"), Value("-5.2"))))
        },
        test("zero longitude and latitude") {
          for {
            result <- ZIO.attempt(LongLatInput.encode(LongLat(0d, 0d)))
          } yield assert(result)(equalTo(RespCommand(Value("0.0"), Value("0.0"))))
        }
      ),
      suite("MemberScore")(
        test("with positive score and empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("", 4.2d)))
          } yield assert(result)(equalTo(RespCommand(Value("4.2"), Value(""))))
        },
        test("with positive score in scientific notation and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("member", 3.141592e100)))
          } yield assert(result)(equalTo(RespCommand(Value("3.141592e100"), Value("member"))))
        },
        test("with negative score and empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("", -4.2d)))
          } yield assert(result)(equalTo(RespCommand(Value("-4.2"), Value(""))))
        },
        test("with zero score and empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("", 0d)))
          } yield assert(result)(equalTo(RespCommand(Value("0.0"), Value(""))))
        },
        test("with positive score and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("member", 4.2d)))
          } yield assert(result)(equalTo(RespCommand(Value("4.2"), Value("member"))))
        },
        test("with negative score and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("member", -4.2d)))
          } yield assert(result)(equalTo(RespCommand(Value("-4.2"), Value("member"))))
        },
        test("with zero score and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("member", 0d)))
          } yield assert(result)(equalTo(RespCommand(Value("0.0"), Value("member"))))
        },
        test("with positive infinity score and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("member", Double.PositiveInfinity)))
          } yield assert(result)(equalTo(RespCommand(Value("+inf"), Value("member"))))
        },
        test("with negative infinity score and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore("member", Double.NegativeInfinity)))
          } yield assert(result)(equalTo(RespCommand(Value("-inf"), Value("member"))))
        }
      ),
      suite("NoInput")(
        test("valid value") {
          for {
            result <- ZIO.attempt(NoInput.encode(()))
          } yield assert(result.args)(isEmpty)
        }
      ),
      suite("NonEmptyList")(
        test("with multiple elements") {
          for {
            result <- ZIO.attempt(NonEmptyList(StringInput).encode(("a", List("b", "c"))))
          } yield assert(result)(equalTo(RespCommand(Value("a"), Value("b"), Value("c"))))
        },
        test("with one element") {
          for {
            result <- ZIO.attempt(NonEmptyList(StringInput).encode(("a", List.empty)))
          } yield assert(result)(equalTo(RespCommand(Value("a"))))
        }
      ),
      suite("Order")(
        test("ascending") {
          for {
            result <- ZIO.attempt(OrderInput.encode(Ascending))
          } yield assert(result)(equalTo(RespCommand(Value("ASC"))))
        },
        test("descending") {
          for {
            result <- ZIO.attempt(OrderInput.encode(Descending))
          } yield assert(result)(equalTo(RespCommand(Value("DESC"))))
        }
      ),
      suite("RadiusUnit")(
        test("meters") {
          for {
            result <- ZIO.attempt(RadiusUnitInput.encode(Meters))
          } yield assert(result)(equalTo(RespCommand(Value("m"))))
        },
        test("kilometers") {
          for {
            result <- ZIO.attempt(RadiusUnitInput.encode(Kilometers))
          } yield assert(result)(equalTo(RespCommand(Value("km"))))
        },
        test("feet") {
          for {
            result <- ZIO.attempt(RadiusUnitInput.encode(Feet))
          } yield assert(result)(equalTo(RespCommand(Value("ft"))))
        },
        test("miles") {
          for {
            result <- ZIO.attempt(RadiusUnitInput.encode(Miles))
          } yield assert(result)(equalTo(RespCommand(Value("mi"))))
        }
      ),
      suite("Range")(
        test("with positive start and positive end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(1 to 5))
          } yield assert(result)(equalTo(RespCommand(Value("1"), Value("5"))))
        },
        test("with negative start and positive end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(-1 to 5))
          } yield assert(result)(equalTo(RespCommand(Value("-1"), Value("5"))))
        },
        test("with positive start and negative end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(1 to -5))
          } yield assert(result)(equalTo(RespCommand(Value("1"), Value("-5"))))
        },
        test("with negative start and negative end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(-1 to -5))
          } yield assert(result)(equalTo(RespCommand(Value("-1"), Value("-5"))))
        },
        test("with positive start and exclusive positive end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(1 until 3))
          } yield assert(result)(equalTo(RespCommand(Value("1"), Value("2"))))
        },
        test("with positive start and exclusive negative end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(1 until -1))
          } yield assert(result)(equalTo(RespCommand(Value("1"), Value("-2"))))
        },
        test("with negative start and exclusive positive end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(-5 until 8))
          } yield assert(result)(equalTo(RespCommand(Value("-5"), Value("7"))))
        },
        test("with negative start and exclusive negative end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(-5 until -3))
          } yield assert(result)(equalTo(RespCommand(Value("-5"), Value("-4"))))
        }
      ),
      suite("Pattern")(
        test("with valid pattern") {
          for {
            result <- ZIO.attempt(PatternInput.encode(Pattern("*[ab]-*")))
          } yield assert(result)(equalTo(RespCommand(Literal("MATCH"), Value("*[ab]-*"))))
        },
        test("with empty pattern") {
          for {
            result <- ZIO.attempt(PatternInput.encode(Pattern("")))
          } yield assert(result)(equalTo(RespCommand(Literal("MATCH"), Value(""))))
        }
      ),
      suite("Replace")(
        test("valid value") {
          for {
            result <- ZIO.attempt(ReplaceInput.encode(Replace))
          } yield assert(result)(equalTo(RespCommand(Literal("REPLACE"))))
        }
      ),
      suite("StoreDist")(
        test("with non-empty string") {
          for {
            result <- ZIO.attempt(StoreDistInput.encode(StoreDist("key")))
          } yield assert(result)(equalTo(RespCommand(Literal("STOREDIST"), Value("key"))))
        },
        test("with empty string") {
          for {
            result <- ZIO.attempt(StoreDistInput.encode(StoreDist("")))
          } yield assert(result)(equalTo(RespCommand(Literal("STOREDIST"), Value(""))))
        }
      ),
      suite("Store")(
        test("with non-empty string") {
          for {
            result <- ZIO.attempt(StoreInput.encode(Store("key")))
          } yield assert(result)(equalTo(RespCommand(Literal("STORE"), Value("key"))))
        },
        test("with empty string") {
          for {
            result <- ZIO.attempt(StoreInput.encode(Store("")))
          } yield assert(result)(equalTo(RespCommand(Literal("STORE"), Value(""))))
        }
      ),
      suite("ScoreRange")(
        test("with infinite min and infinite max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Infinity.asString, ScoreMaximum.Infinity.asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("-inf"), Value("+inf"))))
        },
        test("with open min and infinite max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).asString, ScoreMaximum.Infinity.asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("(4.2"), Value("+inf"))))
        },
        test("with closed min and infinite max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).asString, ScoreMaximum.Infinity.asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("4.2"), Value("+inf"))))
        },
        test("with infinite min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Infinity.asString, ScoreMaximum.Open(5.2d).asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("-inf"), Value("(5.2"))))
        },
        test("with open min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).asString, ScoreMaximum.Open(5.2d).asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("(4.2"), Value("(5.2"))))
        },
        test("with closed min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).asString, ScoreMaximum.Open(5.2d).asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("4.2"), Value("(5.2"))))
        },
        test("with infinite min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Infinity.asString, ScoreMaximum.Closed(5.2d).asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("-inf"), Value("5.2"))))
        },
        test("with open min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).asString, ScoreMaximum.Closed(5.2d).asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("(4.2"), Value("5.2"))))
        },
        test("with closed min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryValueInput[String](), ArbitraryValueInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).asString, ScoreMaximum.Closed(5.2d).asString))
                      )
          } yield assert(result)(equalTo(RespCommand(Value("4.2"), Value("5.2"))))
        }
      ),
      suite("ScriptDebug")(
        test("yes") {
          for {
            result <- ZIO.attempt(ScriptDebugInput.encode(DebugMode.Yes))
          } yield assert(result)(equalTo(RespCommand(Literal("YES"))))
        },
        test("sync") {
          for {
            result <- ZIO.attempt(ScriptDebugInput.encode(DebugMode.Sync))
          } yield assert(result)(equalTo(RespCommand(Literal("SYNC"))))
        },
        test("no") {
          for {
            result <- ZIO.attempt(ScriptDebugInput.encode(DebugMode.No))
          } yield assert(result)(equalTo(RespCommand(Literal("NO"))))
        }
      ),
      suite("ScriptFlush")(
        test("asynchronous") {
          for {
            result <- ZIO.attempt(ScriptFlushInput.encode(FlushMode.Async))
          } yield assert(result)(equalTo(RespCommand(Literal("ASYNC"))))
        },
        test("synchronous") {
          for {
            result <- ZIO.attempt(ScriptFlushInput.encode(FlushMode.Sync))
          } yield assert(result)(equalTo(RespCommand(Literal("SYNC"))))
        }
      ),
      suite("String")(
        test("non-empty value") {
          for {
            result <- ZIO.attempt(StringInput.encode("non-empty"))
          } yield assert(result)(equalTo(RespCommand(Value("non-empty"))))
        },
        test("empty value") {
          for {
            result <- ZIO.attempt(StringInput.encode(""))
          } yield assert(result)(equalTo(RespCommand(Value(""))))
        }
      ),
      suite("Optional")(
        test("none") {
          for {
            result <- ZIO.attempt(OptionalInput(LongInput).encode(None))
          } yield assert(result.args)(isEmpty)
        },
        test("some") {
          for {
            result <- ZIO.attempt(OptionalInput(LongInput).encode(Some(2L)))
          } yield assert(result)(equalTo(RespCommand(Value("2"))))
        }
      ),
      suite("TimeSeconds")(
        test("positiv value") {
          for {
            result <- ZIO.attempt(TimeSecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(RespCommand(Value("3"))))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(TimeSecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(RespCommand(Value("0"))))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(TimeSecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(RespCommand(Value("-3"))))
        }
      ),
      suite("TimeMilliseconds")(
        test("positiv value") {
          for {
            result <- ZIO.attempt(TimeMillisecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(RespCommand(Value("3000"))))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(TimeMillisecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(RespCommand(Value("0"))))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(TimeMillisecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(RespCommand(Value("-3000"))))
        }
      ),
      suite("Tuple2")(
        test("valid value") {
          for {
            result <- ZIO.attempt(Tuple2(StringInput, LongInput).encode(("one", 2L)))
          } yield assert(result)(equalTo(RespCommand(Value("one"), Value("2"))))
        }
      ),
      suite("Tuple3")(
        test("valid value") {
          for {
            result <- ZIO.attempt(Tuple3(StringInput, LongInput, StringInput).encode(("one", 2, "three")))
          } yield assert(result)(equalTo(RespCommand(Value("one"), Value("2"), Value("three"))))
        }
      ),
      suite("Tuple4")(
        test("valid value") {
          for {
            result <- ZIO.attempt(Tuple4(StringInput, LongInput, StringInput, LongInput).encode(("one", 2, "three", 4)))
          } yield assert(result)(equalTo(RespCommand(Value("one"), Value("2"), Value("three"), Value("4"))))
        }
      ),
      suite("Tuple5")(
        test("valid value") {
          for {
            result <- ZIO.attempt(
                        Tuple5(StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five"))
                      )
          } yield assert(result)(
            equalTo(RespCommand(Value("one"), Value("2"), Value("three"), Value("4"), Value("five")))
          )
        }
      ),
      suite("Tuple7")(
        test("valid value") {
          for {
            result <- ZIO.attempt(
                        Tuple7(StringInput, LongInput, StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five", 6, "seven"))
                      )
          } yield assert(result)(
            equalTo(
              RespCommand(
                Value("one"),
                Value("2"),
                Value("three"),
                Value("4"),
                Value("five"),
                Value("6"),
                Value("seven")
              )
            )
          )
        }
      ),
      suite("Tuple9")(
        test("valid value") {
          for {
            result <- ZIO.attempt(
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
              RespCommand(
                Value("one"),
                Value("2"),
                Value("three"),
                Value("4"),
                Value("five"),
                Value("6"),
                Value("seven"),
                Value("8"),
                Value("nine")
              )
            )
          )
        }
      ),
      suite("Tuple11")(
        test("valid value") {
          for {
            result <- ZIO.attempt(
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
              RespCommand(
                Value("one"),
                Value("2"),
                Value("three"),
                Value("4"),
                Value("five"),
                Value("6"),
                Value("seven"),
                Value("8"),
                Value("nine"),
                Value("10"),
                Value("eleven")
              )
            )
          )
        }
      ),
      suite("Update")(
        test("set existing") {
          for {
            result <- ZIO.attempt(UpdateInput.encode(Update.SetExisting))
          } yield assert(result)(equalTo(RespCommand(Value("XX"))))
        },
        test("set new") {
          for {
            result <- ZIO.attempt(UpdateInput.encode(Update.SetNew))
          } yield assert(result)(equalTo(RespCommand(Value("NX"))))
        }
      ),
      suite("UpdateByScore")(
        test("set greater than") {
          for {
            result <- ZIO.attempt(UpdateByScoreInput.encode(UpdateByScore.SetGreaterThan))
          } yield assert(result)(equalTo(RespCommand(Value("GT"))))
        },
        test("set less than") {
          for {
            result <- ZIO.attempt(UpdateByScoreInput.encode(UpdateByScore.SetLessThan))
          } yield assert(result)(equalTo(RespCommand(Value("LT"))))
        }
      ),
      suite("Id")(
        test("valid value") {
          for {
            result <- ZIO.attempt(IdInput.encode(10))
          } yield assert(result)(equalTo(RespCommand(Literal("ID"), Value("10"))))
        }
      ),
      suite("IDs")(
        test("with a single element") {
          for {
            result <- ZIO.attempt(IdsInput.encode((1, Nil)))
          } yield assert(result)(equalTo(RespCommand(Literal("ID"), Value("1"))))
        },
        test("with multiple elements") {
          for {
            result <- ZIO.attempt(IdsInput.encode((1, List(2, 3, 4))))
          } yield assert(result)(
            equalTo(RespCommand(Literal("ID"), Value("1"), Value("2"), Value("3"), Value("4")))
          )
        }
      ),
      suite("Varargs")(
        test("with multiple elements") {
          for {
            result <- ZIO.attempt(Varargs(LongInput).encode(List(1, 2, 3)))
          } yield assert(result)(equalTo(RespCommand(Value("1"), Value("2"), Value("3"))))
        },
        test("with no elements") {
          for {
            result <- ZIO.attempt(Varargs(LongInput).encode(List.empty))
          } yield assert(result.args)(isEmpty)
        }
      ),
      suite("WithScore")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithScoreInput.encode(WithScore))
          } yield assert(result)(equalTo(RespCommand(Literal("WITHSCORE"))))
        }
      ),
      suite("WithScores")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithScoresInput.encode(WithScores))
          } yield assert(result)(equalTo(RespCommand(Literal("WITHSCORES"))))
        }
      ),
      suite("WithCoord")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithCoordInput.encode(WithCoord))
          } yield assert(result)(equalTo(RespCommand(Literal("WITHCOORD"))))
        }
      ),
      suite("WithDist")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithDistInput.encode(WithDist))
          } yield assert(result)(equalTo(RespCommand(Literal("WITHDIST"))))
        }
      ),
      suite("WithHash")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithHashInput.encode(WithHash))
          } yield assert(result)(equalTo(RespCommand(Literal("WITHHASH"))))
        }
      ),
      suite("Idle")(
        test("with 1 second") {
          ZIO
            .attempt(IdleInput.encode(1.second))
            .map(assert(_)(equalTo(RespCommand(Literal("IDLE"), Value("1000")))))
        },
        test("with 100 milliseconds") {
          ZIO
            .attempt(IdleInput.encode(100.millis))
            .map(assert(_)(equalTo(RespCommand(Literal("IDLE"), Value("100")))))
        },
        test("with negative duration") {
          ZIO
            .attempt(IdleInput.encode((-1).second))
            .map(assert(_)(equalTo(RespCommand(Literal("IDLE"), Value("-1000")))))
        }
      ),
      suite("Time")(
        test("with 1 second") {
          ZIO
            .attempt(TimeInput.encode(1.second))
            .map(assert(_)(equalTo(RespCommand(Literal("TIME"), Value("1000")))))
        },
        test("with 100 milliseconds") {
          ZIO
            .attempt(TimeInput.encode(100.millis))
            .map(assert(_)(equalTo(RespCommand(Literal("TIME"), Value("100")))))
        },
        test("with negative duration") {
          ZIO
            .attempt(TimeInput.encode((-1).second))
            .map(assert(_)(equalTo(RespCommand(Literal("TIME"), Value("-1000")))))
        }
      ),
      suite("RetryCount")(
        test("with positive count") {
          ZIO
            .attempt(RetryCountInput.encode(100))
            .map(assert(_)(equalTo(RespCommand(Literal("RETRYCOUNT"), Value("100")))))
        },
        test("with negative count") {
          ZIO
            .attempt(RetryCountInput.encode(-100))
            .map(assert(_)(equalTo(RespCommand(Literal("RETRYCOUNT"), Value("-100")))))
        }
      ),
      suite("XGroupCreate")(
        test("without mkStream") {
          ZIO
            .attempt(
              XGroupCreateInput[String, String, String]().encode(
                XGroupCommand.Create("key", "group", "id")
              )
            )
            .map(assert(_)(equalTo(RespCommand(Literal("CREATE"), Key("key"), Value("group"), Value("id")))))
        },
        test("with mkStream") {
          ZIO
            .attempt(
              XGroupCreateInput[String, String, String]().encode(
                XGroupCommand.Create("key", "group", "id")
              )
            )
            .map(
              assert(_)(
                equalTo(
                  RespCommand(Literal("CREATE"), Key("key"), Value("group"), Value("id"))
                )
              )
            )
        }
      ),
      suite("XGroupSetId")(
        test("valid value") {
          ZIO
            .attempt(XGroupSetIdInput[String, String, String]().encode(XGroupCommand.SetId("key", "group", "id")))
            .map(assert(_)(equalTo(RespCommand(Literal("SETID"), Key("key"), Value("group"), Value("id")))))
        },
        test("valid value with entries read") {
          ZIO
            .attempt(XGroupSetIdInput[String, String, String]().encode(XGroupCommand.SetId("key", "group", "id")))
            .map(assert(_)(equalTo(RespCommand(Literal("SETID"), Key("key"), Value("group"), Value("id")))))
        }
      ),
      suite("XGroupDestroy")(
        test("valid value") {
          ZIO
            .attempt(XGroupDestroyInput[String, String]().encode(XGroupCommand.Destroy("key", "group")))
            .map(assert(_)(equalTo(RespCommand(Literal("DESTROY"), Key("key"), Value("group")))))
        }
      ),
      suite("XGroupCreateConsumer")(
        test("valid value") {
          ZIO
            .attempt(
              XGroupCreateConsumerInput[String, String, String]().encode(
                XGroupCommand.CreateConsumer("key", "group", "consumer")
              )
            )
            .map(
              assert(_)(
                equalTo(RespCommand(Literal("CREATECONSUMER"), Key("key"), Value("group"), Value("consumer")))
              )
            )
        }
      ),
      suite("XGroupDelConsumer")(
        test("valid value") {
          ZIO
            .attempt(
              XGroupDelConsumerInput[String, String, String]().encode(
                XGroupCommand.DelConsumer("key", "group", "consumer")
              )
            )
            .map(
              assert(_)(equalTo(RespCommand(Literal("DELCONSUMER"), Key("key"), Value("group"), Value("consumer"))))
            )
        }
      ),
      suite("Block")(
        test("with 1 second") {
          ZIO
            .attempt(BlockInput.encode(1.second))
            .map(assert(_)(equalTo(RespCommand(Literal("BLOCK"), Value("1000")))))
        },
        test("with 100 milliseconds") {
          ZIO
            .attempt(BlockInput.encode(100.millis))
            .map(assert(_)(equalTo(RespCommand(Literal("BLOCK"), Value("100")))))
        },
        test("with negative duration") {
          ZIO
            .attempt(BlockInput.encode((-1).second))
            .map(assert(_)(equalTo(RespCommand(Literal("BLOCK"), Value("-1000")))))
        }
      ),
      suite("Streams")(
        test("with one pair") {
          ZIO
            .attempt(StreamsInput[String, String]().encode(("a" -> "b", Chunk.empty)))
            .map(assert(_)(equalTo(RespCommand(Literal("STREAMS"), Key("a"), Value("b")))))
        },
        test("with multiple pairs") {
          ZIO
            .attempt(StreamsInput[String, String]().encode(("a" -> "b", Chunk.single("c" -> "d"))))
            .map(assert(_)(equalTo(RespCommand(Literal("STREAMS"), Key("a"), Key("c"), Value("b"), Value("d")))))
        }
      ),
      suite("NoAck")(
        test("valid value") {
          ZIO.attempt(NoAckInput.encode(NoAck)).map(assert(_)(equalTo(RespCommand(Value("NOACK")))))
        }
      ),
      suite("CappedStreamOption")(
        test("MaxLen with approx") {
          ZIO
            .attempt(MaxLenApproxInput.encode(CappedStreamType.MaxLenApprox(10, None)))
            .map(assert(_)(equalTo(RespCommand(Literal("MAXLEN"), Literal("~"), Value("10")))))
        },
        test("MaxLen with approx and Limit") {
          ZIO
            .attempt(MaxLenApproxInput.encode(CappedStreamType.MaxLenApprox(10, Some(100))))
            .map(
              assert(_)(
                equalTo(RespCommand(Literal("MAXLEN"), Literal("~"), Value("10"), Literal("LIMIT"), Value("100")))
              )
            )
        },
        test("MaxLen with exact") {
          ZIO
            .attempt(MaxLenExactInput.encode(CappedStreamType.MaxLenExact(10)))
            .map(assert(_)(equalTo(RespCommand(Literal("MAXLEN"), Literal("="), Value("10")))))
        },
        test("MinId with approx") {
          ZIO
            .attempt(MinIdApproxInput[String]().encode(CappedStreamType.MinIdApprox("10", None)))
            .map(assert(_)(equalTo(RespCommand(Literal("MINID"), Literal("~"), Value("10")))))
        },
        test("MinId with approx and Limit") {
          ZIO
            .attempt(MinIdApproxInput[String]().encode(CappedStreamType.MinIdApprox("10", Some(100))))
            .map(
              assert(_)(
                equalTo(RespCommand(Literal("MINID"), Literal("~"), Value("10"), Literal("LIMIT"), Value("100")))
              )
            )
        },
        test("MinId with exact") {
          ZIO
            .attempt(MinIdExactInput[String]().encode(CappedStreamType.MinIdExact("10")))
            .map(assert(_)(equalTo(RespCommand(Literal("MINID"), Literal("="), Value("10")))))
        }
      ),
      suite("WithForce")(
        test("valid value") {
          ZIO.attempt(WithForceInput.encode(WithForce)).map(assert(_)(equalTo(RespCommand(Literal("FORCE")))))
        }
      ),
      suite("WithJustId")(
        test("valid value") {
          ZIO.attempt(WithJustIdInput.encode(WithJustId)).map(assert(_)(equalTo(RespCommand(Literal("JUSTID")))))
        }
      ),
      suite("Side")(
        test("left") {
          for {
            result <- ZIO.attempt(SideInput.encode(Side.Left))
          } yield assert(result)(equalTo(RespCommand(Literal("LEFT"))))
        },
        test("right") {
          for {
            result <- ZIO.attempt(SideInput.encode(Side.Right))
          } yield assert(result)(equalTo(RespCommand(Literal("RIGHT"))))
        }
      ),
      suite("ListMaxLen")(
        test("valid value") {
          ZIO
            .attempt(ListMaxLenInput.encode(ListMaxLen(10L)))
            .map(assert(_)(equalTo(RespCommand(Literal("MAXLEN"), Value("10")))))
        }
      ),
      suite("Rank")(
        test("valid value") {
          ZIO.attempt(RankInput.encode(Rank(10L))).map(assert(_)(equalTo(RespCommand(Literal("RANK"), Value("10")))))
        }
      ),
      suite("GetEx")(
        test("GetExInput - valid value") {
          for {
            resultSeconds              <-
              ZIO.attempt(GetExInput[String]().encode(scala.Tuple2("key", GetExpire.Seconds(PositiveLong(1)))))
            resultMilliseconds         <-
              ZIO.attempt(
                GetExInput[String]().encode(scala.Tuple2("key", GetExpire.Milliseconds(PositiveLong(100))))
              )
            unixTimeSeconds             = unsafeWrap(PositiveLong)(Instant.parse("2021-04-06T00:00:00Z").getEpochSecond)
            resultUnixTimeSeconds      <-
              ZIO.attempt(GetExInput[String]().encode(scala.Tuple2("key", GetExpire.UnixTimeSeconds(unixTimeSeconds))))
            unixTimeMilliseconds        = unsafeWrap(PositiveLong)(Instant.parse("2021-04-06T00:00:00Z").toEpochMilli)
            resultUnixTimeMilliseconds <-
              ZIO.attempt(
                GetExInput[String]().encode(scala.Tuple2("key", GetExpire.UnixTimeMilliseconds(unixTimeMilliseconds)))
              )
            resultPersist              <- ZIO.attempt(GetExInput[String]().encode(scala.Tuple2("key", GetExpire.Persist)))
          } yield assertTrue(
            resultSeconds == RespCommand(Key("key"), Literal("EX"), Value("1")),
            resultMilliseconds == RespCommand(Key("key"), Literal("PX"), Value("100")),
            resultUnixTimeSeconds == RespCommand(Key("key"), Literal("EXAT"), Value("1617667200")),
            resultUnixTimeMilliseconds == RespCommand(Key("key"), Literal("PXAT"), Value("1617667200000")),
            resultPersist == RespCommand(Key("key"), Literal("PERSIST"))
          )
        }
      )
    )
}
