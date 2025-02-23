package zio.redis

import zio._
import zio.prelude.Newtype.unsafeWrap
import zio.redis.RedisError.{ProtocolError, WrongType}
import zio.redis.options.PositiveLong
import zio.test.Assertion.{exists => _, _}
import zio.test.TestAspect.flaky
import zio.test._

trait StringsSpec extends IntegrationSpec {
  def stringsSuite: Spec[Redis, RedisError] =
    suite("strings")(
      suite("append")(
        test("to the end of non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "val")
            len   <- redis.append(key, "ue")
          } yield assert(len)(equalTo(5L))
        },
        test("to the end of empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            len   <- redis.append(key, "value")
          } yield assert(len)(equalTo(5L))
        },
        test("empty value to the end of non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            len   <- redis.append(key, "")
          } yield assert(len)(equalTo(5L))
        },
        test("empty value to the end of empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            len   <- redis.append(key, "")
          } yield assert(len)(equalTo(0L))
        },
        test("error when not string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            len   <- redis.append(key, "b").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("bitCount")(
        test("over non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            count <- redis.bitCount(key)
          } yield assert(count)(equalTo(21L))
        },
        test("over empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            count <- redis.bitCount(key)
          } yield assert(count)(equalTo(0L))
        },
        test("error when not string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            count <- redis.bitCount(key).either
          } yield assert(count)(isLeft(isSubtype[WrongType](anything)))
        },
        test("over non-empty string with range") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value") // "alu" => 01100001 01101100 01110101
            count <- redis.bitCount(key, Some(1 to 3))
          } yield assert(count)(equalTo(12L))
        },
        test("over non-empty string with exclusive range") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            count <- redis.bitCount(key, Some(1 until 4))
          } yield assert(count)(equalTo(12L))
        },
        test("over non-empty string with range that is too large") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            count <- redis.bitCount(key, Some(1 to 20))
          } yield assert(count)(equalTo(16L))
        },
        test("over non-empty string with range that ends with the string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            count <- redis.bitCount(key, Some(1 to -1))
          } yield assert(count)(equalTo(16L))
        },
        test("over non-empty string with range whose start is bigger than end") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            count <- redis.bitCount(key, Some(3 to 1))
          } yield assert(count)(equalTo(0L))
        },
        test("over empty string with range") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            count <- redis.bitCount(key, Some(1 to 3))
          } yield assert(count)(equalTo(0L))
        },
        test("over not string with range") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            count <- redis.bitCount(key, Some(1 to 3)).either
          } yield assert(count)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("bitField")(
        test("get second byte with signed 8 type from existing value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.SignedInt(8), 8))
          } yield assert(result)(equalTo(Chunk(Some(97L))))
        },
        test("get second byte with unsigned 8 type from existing value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(8), 8))
          } yield assert(result)(equalTo(Chunk(Some(97L))))
        },
        test("get bit with offset out of range from existing value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(1), 100))
          } yield assert(result)(equalTo(Chunk(Some(0L))))
        },
        test("get bit when empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(1), 10))
          } yield assert(result)(equalTo(Chunk(Some(0L))))
        },
        test("get error when negative offset") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(1), -10)).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("get error when not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(1), 10)).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("set second byte when non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "vblue")
            result <- redis.bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(8), 8, 97))
          } yield assert(result)(equalTo(Chunk(Some(98L))))
        },
        test("set bit when offset out of range") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(8), 100, 97))
          } yield assert(result)(equalTo(Chunk(Some(0L))))
        },
        test("set negative value when unsigned type and existing string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(4), 10, -10))
          } yield assert(result)(equalTo(Chunk(Some(8L))))
        },
        test("set too big value when unsigned type and existing string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(2), 10, 100))
          } yield assert(result)(equalTo(Chunk(Some(2L))))
        },
        test("set error when negative offset") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(1), -10, 10)).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set error when not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(8), 8, 97)).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("increment byte when non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 3))
          } yield assert(result)(equalTo(Chunk(Some(100L))))
        },
        test("increment byte by negative value when non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, -2))
          } yield assert(result)(equalTo(Chunk(Some(95L))))
        },
        test("increment bit when offset out of range") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(1), 100, 1))
          } yield assert(result)(equalTo(Chunk(Some(1L))))
        },
        test("increment bit when value will overflow") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 200))
          } yield assert(result)(equalTo(Chunk(Some(41L))))
        },
        test("increment error when negative offset") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(1), -10, 1)).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("increment error when not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(1), 10, 1)).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("increment with overflow saturation") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(
                        key,
                        BitFieldCommand.BitFieldOverflow.Sat,
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 200)
                      )
          } yield assert(result)(equalTo(Chunk(Some(255L))))
        },
        test("increment with overflow fail") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(
                        key,
                        BitFieldCommand.BitFieldOverflow.Fail,
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 200)
                      )
          } yield assert(result)(equalTo(Chunk(None)))
        },
        test("increment first with overflow wrap and then overflow fail") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitField(
                        key,
                        BitFieldCommand.BitFieldOverflow.Wrap,
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 200),
                        BitFieldCommand.BitFieldOverflow.Fail,
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 250)
                      )
          } yield assert(result)(equalTo(Chunk(Some(41L), None)))
        },
        test("first set, then increment and then get same bits") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value", None, None)
            result <- redis.bitField(
                        key,
                        BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(8), 8, 98L),
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 2L),
                        BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(8), 8)
                      )
          } yield assert(result)(equalTo(Chunk(Some(97L), Some(100L), Some(100L))))
        }
      ),
      suite("LCS")(
        test("get LCS from two keys") {
          val str1 = "foo"
          val str2 = "fao"

          for {
            redis  <- ZIO.service[Redis]
            key1   <- uuid
            _      <- redis.set(key1, str1, None, None)
            key2   <- uuid
            _      <- redis.set(key2, str2, None, None)
            result <- redis.lcs(key1, key2)
          } yield assert(result)(equalTo(Lcs.PlainLcs("fo")))
        },
        test("get LCS from unknown keys") {
          val str1 = "foo"
          val str2 = "fao"

          for {
            redis  <- ZIO.service[Redis]
            key1   <- uuid
            _      <- redis.set(key1, str1, None, None)
            key2   <- uuid
            _      <- redis.set(key2, str2, None, None)
            result <- redis.lcs("unknown", "unknown")
          } yield assert(result)(equalTo(Lcs.PlainLcs("")))
        },
        test("get length of LCS") {
          val str1 = "foo"
          val str2 = "fao"

          for {
            redis  <- ZIO.service[Redis]
            key1   <- uuid
            _      <- redis.set(key1, str1, None, None)
            key2   <- uuid
            _      <- redis.set(key2, str2, None, None)
            result <- redis.lcs(key1, key2, Some(LcsQueryType.Len))
          } yield assert(result)(equalTo(Lcs.Length(2)))
        },
        test("get length of LCS for unknown keys") {
          val str1 = "foo"
          val str2 = "fao"

          for {
            redis  <- ZIO.service[Redis]
            key1   <- uuid
            _      <- redis.set(key1, str1, None, None)
            key2   <- uuid
            _      <- redis.set(key2, str2, None, None)
            result <- redis.lcs("unknown", "unknown", Some(LcsQueryType.Len))
          } yield assert(result)(equalTo(Lcs.Length(0)))
        },
        test("get index of LCS") {
          val str1 = "!ohmytext"
          val str2 = "!mynewtext"

          for {
            redis  <- ZIO.service[Redis]
            key1   <- uuid
            _      <- redis.set(key1, str1)
            key2   <- uuid
            _      <- redis.set(key2, str2)
            result <- redis.lcs(key1, key2, Some(LcsQueryType.Idx()))
          } yield assert(result)(
            equalTo(
              Lcs.Matches(
                List(
                  Match(matchIdxA = MatchIdx(5, 8), matchIdxB = MatchIdx(6, 9)),
                  Match(matchIdxA = MatchIdx(3, 4), matchIdxB = MatchIdx(1, 2)),
                  Match(matchIdxA = MatchIdx(0, 0), matchIdxB = MatchIdx(0, 0))
                ),
                7
              )
            )
          )
        },
        test("get index of LCS with MINMATCHLEN") {
          val str1 = "!ohmytext"
          val str2 = "!mynewtext"

          for {
            redis  <- ZIO.service[Redis]
            key1   <- uuid
            _      <- redis.set(key1, str1)
            key2   <- uuid
            _      <- redis.set(key2, str2)
            result <- redis.lcs(key1, key2, Some(LcsQueryType.Idx(minMatchLength = 2)))
          } yield assert(result)(
            equalTo(
              Lcs.Matches(
                List(
                  Match(matchIdxA = MatchIdx(5, 8), matchIdxB = MatchIdx(6, 9)),
                  Match(matchIdxA = MatchIdx(3, 4), matchIdxB = MatchIdx(1, 2))
                ),
                7
              )
            )
          )
        },
        test("get index of LCS with WITHMATCHLEN") {
          val str1 = "!ohmytext"
          val str2 = "!mynewtext"

          for {
            redis  <- ZIO.service[Redis]
            key1   <- uuid
            _      <- redis.set(key1, str1)
            key2   <- uuid
            _      <- redis.set(key2, str2)
            result <- redis.lcs(key1, key2, Some(LcsQueryType.Idx(withMatchLength = true)))
          } yield assert(result)(
            equalTo(
              Lcs.Matches(
                List(
                  Match(matchIdxA = MatchIdx(5, 8), matchIdxB = MatchIdx(6, 9), matchLength = Some(4)),
                  Match(matchIdxA = MatchIdx(3, 4), matchIdxB = MatchIdx(1, 2), matchLength = Some(2)),
                  Match(matchIdxA = MatchIdx(0, 0), matchIdxB = MatchIdx(0, 0), matchLength = Some(1))
                ),
                7
              )
            )
          )
        },
        test("get index of LCS both with MINMATCHLEN and WITHMATCHLEN") {
          val str1 = "!ohmytext"
          val str2 = "!mynewtext"

          for {
            redis  <- ZIO.service[Redis]
            key1   <- uuid
            _      <- redis.set(key1, str1)
            key2   <- uuid
            _      <- redis.set(key2, str2)
            result <- redis.lcs(key1, key2, Some(LcsQueryType.Idx(minMatchLength = 2, withMatchLength = true)))
          } yield assert(result)(
            equalTo(
              Lcs.Matches(
                List(
                  Match(matchIdxA = MatchIdx(5, 8), matchIdxB = MatchIdx(6, 9), matchLength = Some(4)),
                  Match(matchIdxA = MatchIdx(3, 4), matchIdxB = MatchIdx(1, 2), matchLength = Some(2))
                ),
                7
              )
            )
          )
        }
      ) @@ clusterExecutorUnsupported,
      suite("bitOp")(
        test("AND over multiple non-empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.set(first, "a")
            _      <- redis.set(second, "ab")
            _      <- redis.set(third, "abc")
            result <- redis.bitOp(BitOperation.AND, dest, first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        test("AND over two non-empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.set(first, "first")
            _      <- redis.set(second, "second")
            result <- redis.bitOp(BitOperation.AND, dest, first, second)
          } yield assert(result)(equalTo(6L))
        },
        test("AND over one empty and one non-empty string") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- redis.set(nonEmpty, "value")
            result   <- redis.bitOp(BitOperation.AND, dest, empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        test("AND over two empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- redis.bitOp(BitOperation.AND, dest, first, second)
          } yield assert(result)(equalTo(0L))
        },
        test("error when AND over one empty and one not string") {
          for {
            redis     <- ZIO.service[Redis]
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- redis.sAdd(notString, "a")
            result    <- redis.bitOp(BitOperation.AND, dest, empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when AND over non-empty and one not string") {
          for {
            redis     <- ZIO.service[Redis]
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- redis.sAdd(notString, "a")
            result    <- redis.bitOp(BitOperation.AND, dest, nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("OR over multiple non-empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.set(first, "a")
            _      <- redis.set(second, "ab")
            _      <- redis.set(third, "abc")
            result <- redis.bitOp(BitOperation.OR, dest, first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        test("OR over two non-empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.set(first, "first")
            _      <- redis.set(second, "second")
            result <- redis.bitOp(BitOperation.OR, dest, first, second)
          } yield assert(result)(equalTo(6L))
        },
        test("OR over one empty and one non-empty string") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- redis.set(nonEmpty, "value")
            result   <- redis.bitOp(BitOperation.OR, dest, empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        test("OR over two empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- redis.bitOp(BitOperation.OR, dest, first, second)
          } yield assert(result)(equalTo(0L))
        },
        test("error when OR over one empty and one not string") {
          for {
            redis     <- ZIO.service[Redis]
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- redis.sAdd(notString, "a")
            result    <- redis.bitOp(BitOperation.OR, dest, empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when OR over non-empty and one not string") {
          for {
            redis     <- ZIO.service[Redis]
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- redis.sAdd(notString, "a")
            result    <- redis.bitOp(BitOperation.OR, dest, nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("XOR over multiple non-empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.set(first, "a")
            _      <- redis.set(second, "ab")
            _      <- redis.set(third, "abc")
            result <- redis.bitOp(BitOperation.XOR, dest, first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        test("XOR over two non-empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.set(first, "first")
            _      <- redis.set(second, "second")
            result <- redis.bitOp(BitOperation.XOR, dest, first, second)
          } yield assert(result)(equalTo(6L))
        },
        test("XOR over one empty and one non-empty string") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- redis.set(nonEmpty, "value")
            result   <- redis.bitOp(BitOperation.XOR, dest, empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        test("XOR over two empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- redis.bitOp(BitOperation.XOR, dest, first, second)
          } yield assert(result)(equalTo(0L))
        },
        test("error when XOR over one empty and one not string") {
          for {
            redis     <- ZIO.service[Redis]
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- redis.sAdd(notString, "a")
            result    <- redis.bitOp(BitOperation.XOR, dest, empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when XOR over non-empty and one not string") {
          for {
            redis     <- ZIO.service[Redis]
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- redis.sAdd(notString, "a")
            result    <- redis.bitOp(BitOperation.XOR, dest, nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when NOT over multiple non-empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.set(first, "a")
            _      <- redis.set(second, "ab")
            _      <- redis.set(third, "abc")
            result <- redis.bitOp(BitOperation.NOT, dest, first, second, third).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when NOT over one non-empty and one empty string") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.set(nonEmpty, "a")
            result   <- redis.bitOp(BitOperation.NOT, dest, nonEmpty, empty).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("NOT over non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.bitOp(BitOperation.NOT, dest, key)
          } yield assert(result)(equalTo(5L))
        },
        test("NOT over empty string") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            key    <- uuid
            result <- redis.bitOp(BitOperation.NOT, dest, key)
          } yield assert(result)(equalTo(0L))
        },
        test("error when NOT over not string") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.bitOp(BitOperation.NOT, dest, key).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("bitPos")(
        test("of 1 when non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = true)
          } yield assert(pos)(equalTo(1L))
        },
        test("of 0 when non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = false)
          } yield assert(pos)(equalTo(0L))
        },
        test("of 1 when empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pos   <- redis.bitPos(key, bit = true)
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pos   <- redis.bitPos(key, bit = false)
          } yield assert(pos)(equalTo(0L))
        },
        test("of 1 when non-empty string with start") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(2L, None)))
          } yield assert(pos)(equalTo(17L))
        },
        test("of 0 when non-empty string with start") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(2L, None)))
          } yield assert(pos)(equalTo(16L))
        },
        test("of 1 when start is greater than non-empty string length") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when start is greater than non-empty string length") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 1 when empty string with start") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(1L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when empty string with start") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(0L))
        },
        test("of 1 when non-empty string with start and end") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(17L))
        },
        test("of 0 when non-empty string with start and end") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(16L))
        },
        test("of 1 when start is greater than end with non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when start is greater than end with non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 1 when range is out of non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(10L, Some(15L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when range is out of non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(10L, Some(15L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 1 when start is equal to end with non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(1L, Some(1L))))
          } yield assert(pos)(equalTo(9L))
        },
        test("of 0 when start is equal to end with non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(1L, Some(1L))))
          } yield assert(pos)(equalTo(8L))
        },
        test("of 1 when empty string with start and end") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when empty string with start and end") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(0L))
        },
        test("of 1 when start is greater than end with empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pos   <- redis.bitPos(key, bit = true, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when start is greater than end with empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(0L))
        },
        test("error when not string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            pos   <- redis.bitPos(key, bit = true).either
          } yield assert(pos)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when not string and start is greater than end") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            pos   <- redis.bitPos(key, bit = false, Some(BitPosRange(4L, Some(2L)))).either
          } yield assert(pos)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("decr")(
        test("non-empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "5")
            result <- redis.decr(key)
          } yield assert(result)(equalTo(4L))
        },
        test("empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.decr(key)
          } yield assert(result)(equalTo(-1L))
        },
        test("error when out of range integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "234293482390480948029348230948")
            result <- redis.decr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "not-integer")
            result <- redis.decr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("decrBy")(
        test("3 when non-empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "10")
            result <- redis.decrBy(key, 3L)
          } yield assert(result)(equalTo(7L))
        },
        test("3 when empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.decrBy(key, 3L)
          } yield assert(result)(equalTo(-3L))
        },
        test("-3 when non-empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "10")
            result <- redis.decrBy(key, -3L)
          } yield assert(result)(equalTo(13L))
        },
        test("error when out of range integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "234293482390480948029348230948")
            result <- redis.decrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "not-integer")
            result <- redis.decrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("get")(
        test("non-emtpy string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            result <- redis.get(key).returning[String]
          } yield assert(result)(isSome(equalTo("value")))
        },
        test("non existent string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.get(key).returning[String]
          } yield assert(result)(isNone)
        },
        test("empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "")
            result <- redis.get(key).returning[String]
          } yield assert(result)(isSome(equalTo("")))
        },
        test("error when not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.get(key).returning[String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getBit")(
        test("from non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            bit   <- redis.getBit(key, 17L)
          } yield assert(bit)(equalTo(1L))
        },
        test("with offset larger then string length") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            bit   <- redis.getBit(key, 100L)
          } yield assert(bit)(equalTo(0L))
        },
        test("with empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            bit   <- redis.getBit(key, 10L)
          } yield assert(bit)(equalTo(0L))
        },
        test("error when negative offset") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            bit   <- redis.getBit(key, -1L).either
          } yield assert(bit)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            bit   <- redis.getBit(key, 10L).either
          } yield assert(bit)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getRange")(
        test("from non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            substr <- redis.getRange(key, 1 to 3).returning[String]
          } yield assert(substr)(isSome(equalTo("alu")))
        },
        test("from non-empty string by exclusive range") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            substr <- redis.getRange(key, 1 until 3).returning[String]
          } yield assert(substr)(isSome(equalTo("al")))
        },
        test("with range that exceeds non-empty string length") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            substr <- redis.getRange(key, 1 to 10).returning[String]
          } yield assert(substr)(isSome(equalTo("alue")))
        },
        test("with range that is outside of non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            substr <- redis.getRange(key, 10 to 15).returning[String]
          } yield assert(substr)(isSome(equalTo("")))
        },
        test("with inverse range of non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            substr <- redis.getRange(key, 15 to 3).returning[String]
          } yield assert(substr)(isSome(equalTo("")))
        },
        test("with negative range end from non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            substr <- redis.getRange(key, 1 to -1).returning[String]
          } yield assert(substr)(isSome(equalTo("alue")))
        },
        test("with start and end equal from non-empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            substr <- redis.getRange(key, 1 to 1).returning[String]
          } yield assert(substr)(isSome(equalTo("a")))
        },
        test("from empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            substr <- redis.getRange(key, 1 to 3).returning[String]
          } yield assert(substr)(isSome(equalTo("")))
        },
        test("error when not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            substr <- redis.getRange(key, 1 to 3).returning[String].either
          } yield assert(substr)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getSet")(
        test("non-empty value to the existing string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            oldVal <- redis.getSet(key, "abc").returning[String]
          } yield assert(oldVal)(isSome(equalTo("value")))
        },
        test("empty value to the existing string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            oldVal <- redis.getSet(key, "").returning[String]
          } yield assert(oldVal)(isSome(equalTo("value")))
        },
        test("non-empty value to the empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            oldVal <- redis.getSet(key, "value").returning[String]
          } yield assert(oldVal)(isNone)
        },
        test("empty value to the empty string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            oldVal <- redis.getSet(key, "").returning[String]
          } yield assert(oldVal)(isNone)
        },
        test("error when not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            oldVal <- redis.getSet(key, "value").returning[String].either
          } yield assert(oldVal)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("incr")(
        test("non-empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "5")
            result <- redis.incr(key)
          } yield assert(result)(equalTo(6L))
        },
        test("empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.incr(key)
          } yield assert(result)(equalTo(1L))
        },
        test("error when out of range integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "234293482390480948029348230948")
            result <- redis.incr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "not-integer")
            result <- redis.incr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("incrBy")(
        test("3 when non-empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "10")
            result <- redis.incrBy(key, 3L)
          } yield assert(result)(equalTo(13L))
        },
        test("3 when empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.incrBy(key, 3L)
          } yield assert(result)(equalTo(3L))
        },
        test("-3 when non-empty integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "10")
            result <- redis.incrBy(key, -3L)
          } yield assert(result)(equalTo(7L))
        },
        test("error when out of range integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "234293482390480948029348230948")
            result <- redis.incrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "not-integer")
            result <- redis.incrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("incrByFloat")(
        test("3.4 when non-empty float") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "5.1")
            result <- redis.incrByFloat(key, 3.4d)
          } yield assert(result)(equalTo(8.5))
        },
        test("3.4 when empty float") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.incrByFloat(key, 3.4d)
          } yield assert(result)(equalTo(3.4))
        },
        test("-3.4 when non-empty float") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "5")
            result <- redis.incrByFloat(key, -3.4d)
          } yield assert(result)(equalTo(1.6))
        },
        test("error when out of range value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, s"${Double.MaxValue.toString}1234")
            result <- redis.incrByFloat(key, 3d).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "not-integer")
            result <- redis.incrByFloat(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("mGet")(
        test("from multiple non-empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _      <- redis.set(first, "1")
            _      <- redis.set(second, "2")
            result <- redis.mGet(first, second).returning[String]
          } yield assert(result)(equalTo(Chunk(Some("1"), Some("2"))))
        },
        test("from one non-empty and one empty string") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.set(nonEmpty, "value")
            result   <- redis.mGet(nonEmpty, empty).returning[String]
          } yield assert(result)(equalTo(Chunk(Some("value"), None)))
        },
        test("from two empty strings") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            result <- redis.mGet(first, second).returning[String]
          } yield assert(result)(equalTo(Chunk(None, None)))
        },
        test("from one string and one not string") {
          for {
            redis  <- ZIO.service[Redis]
            str    <- uuid
            notStr <- uuid
            _      <- redis.set(str, "value")
            _      <- redis.sAdd(notStr, "a")
            result <- redis.mGet(str, notStr).returning[String]
          } yield assert(result)(equalTo(Chunk(Some("value"), None)))
        },
        test("from one not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.mGet(key).returning[String]
          } yield assert(result)(equalTo(Chunk(None)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("mSet")(
        test("one new value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.mSet((key, value))
            result <- redis.get(key).returning[String]
          } yield assert(result)(isSome(equalTo(value)))
        },
        test("multiple new values") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            second    <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            _         <- redis.mSet((first, firstVal), (second, secondVal))
            result    <- redis.mGet(first, second).returning[String]
          } yield assert(result)(equalTo(Chunk(Some(firstVal), Some(secondVal))))
        },
        test("replace existing values") {
          for {
            redis       <- ZIO.service[Redis]
            first       <- uuid
            second      <- uuid
            firstVal    <- uuid
            secondVal   <- uuid
            replacement <- uuid
            _           <- redis.mSet((first, firstVal), (second, secondVal))
            _           <- redis.mSet((first, replacement), (second, replacement))
            result      <- redis.mGet(first, second).returning[String]
          } yield assert(result)(equalTo(Chunk(Some(replacement), Some(replacement))))
        },
        test("one value multiple times at once") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            _         <- redis.mSet((key, firstVal), (key, secondVal))
            result    <- redis.get(key).returning[String]
          } yield assert(result)(isSome(equalTo(secondVal)))
        },
        test("replace not string value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.mSet((key, value)).either
          } yield assert(result)(isRight)
        }
      ) @@ clusterExecutorUnsupported,
      suite("mSetNx")(
        test("one new value") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            set   <- redis.mSetNx((key, value))
          } yield assert(set)(isTrue)
        },
        test("multiple new values") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            second    <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            set       <- redis.mSetNx((first, firstVal), (second, secondVal))
          } yield assert(set)(isTrue)
        },
        test("replace existing values") {
          for {
            redis       <- ZIO.service[Redis]
            first       <- uuid
            second      <- uuid
            firstVal    <- uuid
            secondVal   <- uuid
            replacement <- uuid
            _           <- redis.mSetNx((first, firstVal), (second, secondVal))
            set         <- redis.mSetNx((first, replacement), (second, replacement))
          } yield assert(set)(isFalse)
        },
        test("one value multiple times at once") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            set       <- redis.mSetNx((key, firstVal), (key, secondVal))
          } yield assert(set)(isTrue)
        },
        test("replace not string value") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.sAdd(key, "a")
            set   <- redis.mSetNx((key, value))
          } yield assert(set)(isFalse)
        }
      ) @@ clusterExecutorUnsupported,
      suite("set")(
        test("new value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            result <- redis.set(key, value)
          } yield assert(result)(isTrue)
        },
        test("override existing string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, "value")
            result <- redis.set(key, value)
          } yield assert(result)(isTrue)
        },
        test("override not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.set(key, value)
          } yield assert(result)(isTrue)
        },
        test("new value with ttl 1 second") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            result <- redis.set(key, value, expireAt = Some(SetExpire.Seconds(PositiveLong(1))))
          } yield assert(result)(isTrue)
        },
        test("new value with ttl 100 milliseconds") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            result <- redis.set(key, value, expireAt = Some(SetExpire.Milliseconds(PositiveLong(100))))
          } yield assert(result)(isTrue)
        },
        test("new value with SetNew parameter") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            result <- redis.set(key, value, update = Some(Update.SetNew))
          } yield assert(result)(isTrue)
        },
        test("existing value with SetNew parameter") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, "value")
            result <- redis.set(key, value, update = Some(Update.SetNew))
          } yield assert(result)(isFalse)
        },
        test("new value with SetExisting parameter") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            result <- redis.set(key, value, update = Some(Update.SetExisting))
          } yield assert(result)(isFalse)
        },
        test("existing value with SetExisting parameter") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, "value")
            result <- redis.set(key, value, update = Some(Update.SetExisting))
          } yield assert(result)(isTrue)
        },
        test("existing not string value with SetExisting parameter") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.set(key, value, update = Some(Update.SetExisting))
          } yield assert(result)(isTrue)
        },
        // next two tests include KEEPTTL parameter that is valid for Redis version >= 6
        test("new value with KeepTtl parameter") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            result <- redis.set(key, value, expireAt = Some(SetExpire.KeepTtl))
          } yield assert(result)(isTrue)
        },
        test("existing value with KeepTtl parameter") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, "value", expireAt = Some(SetExpire.Seconds(PositiveLong(1))))
            result <- redis.set(key, value, expireAt = Some(SetExpire.KeepTtl))
          } yield assert(result)(isTrue)
        }
      ),
      suite("setBit")(
        test("for existing key") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            oldBit <- redis.setBit(key, 16L, value = true)
          } yield assert(oldBit)(isFalse)
        },
        test("for non-existent key") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            oldBit <- redis.setBit(key, 16L, value = true)
          } yield assert(oldBit)(isFalse)
        },
        test("for offset that is out of string range") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            oldBit <- redis.setBit(key, 100L, value = true)
          } yield assert(oldBit)(isFalse)
        },
        test("error when negative offset") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.set(key, "value")
            oldBit <- redis.setBit(key, -1L, value = false).either
          } yield assert(oldBit)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a")
            oldBit <- redis.setBit(key, 10L, value = true).either
          } yield assert(oldBit)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("setEx")(
        test("new value with 1 second ttl") {
          for {
            redis        <- ZIO.service[Redis]
            key          <- uuid
            value        <- uuid
            _            <- redis.set(key, value, expireAt = Some(SetExpire.Seconds(PositiveLong(1))))
            existsBefore <- redis.exists(key)
            fiber        <- ZIO.sleep(1010.millis).fork <* TestClock.adjust(1010.millis)
            _            <- fiber.join
            existsAfter  <- redis.exists(key)
          } yield assert(existsBefore)(equalTo(1L)) && assert(existsAfter)(equalTo(0L))
        } @@ flaky,
        test("existing value with 1 second ttl") {
          for {
            redis        <- ZIO.service[Redis]
            key          <- uuid
            value        <- uuid
            _            <- redis.set(key, "value")
            _            <- redis.set(key, value, expireAt = Some(SetExpire.Seconds(PositiveLong(1))))
            existsBefore <- redis.exists(key)
            fiber        <- ZIO.sleep(1010.millis).fork <* TestClock.adjust(1010.millis)
            _            <- fiber.join
            existsAfter  <- redis.exists(key)
          } yield assert(existsBefore)(equalTo(1L)) && assert(existsAfter)(equalTo(0L))
        } @@ flaky,
        test("override when not string") {
          for {
            redis        <- ZIO.service[Redis]
            key          <- uuid
            value        <- uuid
            _            <- redis.sAdd(key, "a")
            _            <- redis.set(key, value, expireAt = Some(SetExpire.Seconds(PositiveLong(1))))
            existsBefore <- redis.exists(key)
            fiber        <- ZIO.sleep(1010.millis).fork <* TestClock.adjust(1010.millis)
            _            <- fiber.join
            existsAfter  <- redis.exists(key)
          } yield assert(existsBefore)(equalTo(1L)) && assert(existsAfter)(equalTo(0L))
        }
      ),
      suite("setNx")(
        test("new value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            result <- redis.set(key, value, update = Some(Update.SetNew))
          } yield assert(result)(isTrue)
        },
        test("existing value") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, "value")
            result <- redis.set(key, value, update = Some(Update.SetNew))
          } yield assert(result)(isFalse)
        },
        test("not string") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.sAdd(key, "a")
            result <- redis.set(key, value, update = Some(Update.SetNew))
          } yield assert(result)(isFalse)
        }
      ),
      suite("setRange")(
        test("in existing string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "val")
            len   <- redis.setRange(key, 3L, "ue")
          } yield assert(len)(equalTo(5L))
        },
        test("in non-existent string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            len   <- redis.setRange(key, 2L, "value")
          } yield assert(len)(equalTo(7L))
        },
        test("when offset is larger then string length") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            len   <- redis.setRange(key, 7L, "value")
          } yield assert(len)(equalTo(12L))
        },
        test("error when negative offset") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            len   <- redis.setRange(key, -1L, "value").either
          } yield assert(len)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            len   <- redis.setRange(key, 1L, "value").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("strLen")(
        test("for non-empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "value")
            len   <- redis.strLen(key)
          } yield assert(len)(equalTo(5L))
        },
        test("for empty string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            len   <- redis.strLen(key)
          } yield assert(len)(equalTo(0L))
        },
        test("error when not string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            len   <- redis.strLen(key).either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getEx")(
        test("not found value when set seconds ttl") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, value)
            exists <- redis.getEx(key, GetExpire.Seconds(PositiveLong(1))).returning[String]
            fiber  <- ZIO.sleep(1020.millis).fork <* TestClock.adjust(1020.millis)
            _      <- fiber.join
            res    <- redis.get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(false)) && assert(exists)(isSome(equalTo(value)))
        } @@ flaky,
        test("not found value when set milliseconds ttl") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, value)
            exists <- redis.getEx(key, GetExpire.Milliseconds(PositiveLong(10))).returning[String]
            fiber  <- ZIO.sleep(20.millis).fork <* TestClock.adjust(20.millis)
            _      <- fiber.join
            res    <- redis.get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(false)) && assert(exists)(isSome(equalTo(value)))
        } @@ flaky,
        test("not found value when set seconds timestamp") {
          for {
            redis           <- ZIO.service[Redis]
            key             <- uuid
            value           <- uuid
            _               <- redis.set(key, value)
            expiresAt       <- Clock.instant.map(_.plusMillis(10.millis.toMillis))
            expiresAtSeconds = unsafeWrap(PositiveLong)(expiresAt.getEpochSecond)
            exists          <- redis.getEx(key, GetExpire.UnixTimeSeconds(expiresAtSeconds)).returning[String]
            fiber           <- ZIO.sleep(20.millis).fork <* TestClock.adjust(20.millis)
            _               <- fiber.join
            res             <- redis.get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(false)) && assert(exists)(isSome(equalTo(value)))
        } @@ flaky,
        test("not found value when set milliseconds timestamp") {
          for {
            redis          <- ZIO.service[Redis]
            key            <- uuid
            value          <- uuid
            _              <- redis.set(key, value)
            expiresAt      <- Clock.instant.map(_.plusMillis(10.millis.toMillis))
            expiresAtMillis = unsafeWrap(PositiveLong)(expiresAt.toEpochMilli)
            exists         <- redis.getEx(key, GetExpire.UnixTimeMilliseconds(expiresAtMillis)).returning[String]
            fiber          <- ZIO.sleep(20.millis).fork <* TestClock.adjust(20.millis)
            _              <- fiber.join
            res            <- redis.get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(false)) && assert(exists)(isSome(equalTo(value)))
        } @@ flaky,
        test("key not found") {
          for {
            redis          <- ZIO.service[Redis]
            key            <- uuid
            value          <- uuid
            _              <- redis.set(key, value)
            expiresAt      <- Clock.instant.map(_.plusMillis(10.millis.toMillis))
            expiresAtMillis = unsafeWrap(PositiveLong)(expiresAt.toEpochMilli)
            res            <- redis.getEx(value, GetExpire.UnixTimeMilliseconds(expiresAtMillis)).returning[String]
            res2           <- redis.getEx(value, GetExpire.Milliseconds(PositiveLong(10))).returning[String]
          } yield assert(res)(isNone) && assert(res2)(isNone)
        } @@ flaky
      ),
      suite("getDel")(
        test("error when not string") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "a")
            res   <- redis.getDel(key).returning[String].either
          } yield assert(res)(isLeft(isSubtype[WrongType](anything)))
        },
        test("key not exists") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            res   <- redis.getDel(key).returning[String]
          } yield assert(res)(isNone)
        },
        test("get and remove key") {
          for {
            redis    <- ZIO.service[Redis]
            key      <- uuid
            value    <- uuid
            _        <- redis.set(key, value)
            res      <- redis.getDel(key).returning[String]
            notFound <- redis.getDel(key).returning[String]
          } yield assert(res)(isSome(equalTo(value))) && assert(notFound)(isNone)
        }
      )
    )
}
