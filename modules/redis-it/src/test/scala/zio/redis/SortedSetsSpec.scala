package zio.redis

import zio._
import zio.redis.RedisError.{ProtocolError, WrongType}
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

trait SortedSetsSpec extends IntegrationSpec {
  def sortedSetsSuite: Spec[Redis, RedisError] =
    suite("sorted sets")(
      suite("bzPopMax")(
        test("non-empty set")(
          for {
            redis   <- ZIO.service[Redis]
            key1    <- uuid
            key2    <- uuid
            key3    <- uuid
            duration = Duration.fromMillis(1000)
            delhi    = MemberScore("Delhi", 1d)
            mumbai   = MemberScore("Mumbai", 2d)
            london   = MemberScore("London", 3d)
            paris    = MemberScore("Paris", 4d)
            tokyo    = MemberScore("Tokyo", 5d)
            _       <- redis.zAdd(key1)(delhi, tokyo)
            _       <- redis.zAdd(key2)(mumbai, paris)
            _       <- redis.zAdd(key3)(london)
            result  <- redis.bzPopMax(duration, key1, key2, key3).returning[String]
          } yield assert(result)(isSome(equalTo((key1, tokyo))))
        ),
        test("infinity score in set")(
          for {
            redis   <- ZIO.service[Redis]
            key1    <- uuid
            key2    <- uuid
            duration = Duration.fromMillis(1000)
            delhi    = MemberScore("Delhi", 1d)
            london   = MemberScore("London", 3d)
            tokyo    = MemberScore("Tokyo", 5d)
            edge     = MemberScore("The edge of universe", Double.PositiveInfinity)
            _       <- redis.zAdd(key1)(delhi, edge)
            _       <- redis.zAdd(key2)(london, tokyo)
            result  <- redis.bzPopMax(duration, key1, key2).returning[String]
          } yield assert(result)(isSome(equalTo((key1, edge))))
        ),
        test("empty set")(
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            duration = Duration.fromMillis(1000)
            result  <- redis.bzPopMax(duration, key).returning[String]
          } yield assert(result)(isNone)
        )
      ) @@ clusterExecutorUnsupported,
      suite("bzPopMin")(
        test("non-empty set")(
          for {
            redis   <- ZIO.service[Redis]
            key1    <- uuid
            key2    <- uuid
            key3    <- uuid
            duration = Duration.fromMillis(1000)
            delhi    = MemberScore("Delhi", 1d)
            london   = MemberScore("London", 3d)
            paris    = MemberScore("Paris", 4d)
            _       <- redis.zAdd(key2)(delhi, paris)
            _       <- redis.zAdd(key3)(london)
            result  <- redis.bzPopMin(duration, key1, key2, key3).returning[String]
          } yield assert(result)(isSome(equalTo((key2, delhi))))
        ),
        test("negative infinity score in set")(
          for {
            redis   <- ZIO.service[Redis]
            key1    <- uuid
            key2    <- uuid
            duration = Duration.fromMillis(1000)
            delhi    = MemberScore("Delhi", 1d)
            london   = MemberScore("London", 3d)
            paris    = MemberScore("Paris", 4d)
            quark    = MemberScore("Quark", Double.NegativeInfinity)
            _       <- redis.zAdd(key1)(delhi, quark)
            _       <- redis.zAdd(key2)(london, paris)
            result  <- redis.bzPopMin(duration, key1, key2).returning[String]
          } yield assert(result)(isSome(equalTo((key1, quark))))
        ),
        test("empty set")(
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            duration = Duration.fromMillis(1000)
            result  <- redis.bzPopMin(duration, key).returning[String]
          } yield assert(result)(isNone)
        )
      ) @@ clusterExecutorUnsupported,
      suite("zAdd")(
        test("to empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            added <- redis.zAdd(key)(MemberScore(value, 1d))
          } yield assert(added)(equalTo(1L))
        },
        test("to the non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.zAdd(key)(MemberScore(value, 1d))
            value2 <- uuid
            added  <- redis.zAdd(key)(MemberScore(value2, 2d))
          } yield assert(added)(equalTo(1L))
        },
        test("existing element to set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.zAdd(key)(MemberScore(value, 1d))
            added <- redis.zAdd(key)(MemberScore(value, 2d))
          } yield assert(added)(equalTo(0L))
        },
        test("multiple elements to set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            added <- redis.zAdd(key)(MemberScore("a", 1d), MemberScore("b", 3.1415e50), MemberScore("c", 3d))
          } yield assert(added)(equalTo(3L))
        },
        test("multiple elements with negative & positive infinity") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            added <- redis.zAdd(key)(
                       MemberScore("neg infinity", Double.NegativeInfinity),
                       MemberScore("a", 1d),
                       MemberScore("pos infinity", Double.PositiveInfinity)
                     )
          } yield assert(added)(equalTo(3L))
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            added <- redis.zAdd(key)(MemberScore(value, 1d)).either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        },
        test("NX - do not to update existing members, only add new") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore("v1", 1d))
            _      <- redis.zAdd(key)(MemberScore("v2", 2d))
            added  <- redis.zAdd(key, Some(Update.SetNew))(MemberScore("v3", 3d), MemberScore("v2", 22d))
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v1", "v2", "v3")))
        },
        test("XX - update existing members, not add new") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore("v1", 1d))
            _      <- redis.zAdd(key)(MemberScore("v2", 2d))
            added  <- redis.zAdd(key, Some(Update.SetExisting))(MemberScore("v3", 3d), MemberScore("v1", 11d))
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(0L)) && assert(result.toList)(equalTo(List("v2", "v1")))
        },
        test("CH - return number of new and updated members") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore("v1", 1d))
            _      <- redis.zAdd(key)(MemberScore("v2", 2d))
            added  <- redis.zAdd(key, change = Some(Changed))(MemberScore("v3", 3d), MemberScore("v1", 11d))
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(2L)) && assert(result.toList)(equalTo(List("v2", "v3", "v1")))
        },
        test("LT - return number of new members") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore("v1", 3d))
            _      <- redis.zAdd(key)(MemberScore("v2", 4d))
            added  <- redis.zAdd(key, updateByScore = Some(UpdateByScore.SetLessThan))(
                        MemberScore("v3", 1d),
                        MemberScore("v1", 2d)
                      )
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v3", "v1", "v2")))
        },
        test("GT - return number of new members") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore("v1", 1d))
            _      <- redis.zAdd(key)(MemberScore("v2", 2d))
            added  <- redis.zAdd(key, updateByScore = Some(UpdateByScore.SetGreaterThan))(
                        MemberScore("v3", 1d),
                        MemberScore("v1", 3d)
                      )
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v3", "v2", "v1")))
        },
        test("GT CH - return number of new and updated members") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore("v1", 1d))
            _      <- redis.zAdd(key)(MemberScore("v2", 2d))
            added  <- redis.zAdd(key, updateByScore = Some(UpdateByScore.SetGreaterThan), change = Some(Changed))(
                        MemberScore("v3", 1d),
                        MemberScore("v1", 3d)
                      )
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(2L)) && assert(result.toList)(equalTo(List("v3", "v2", "v1")))
        },
        test("INCR - increment by score") {
          for {
            redis    <- ZIO.service[Redis]
            key      <- uuid
            _        <- redis.zAdd(key)(MemberScore("v1", 1d))
            _        <- redis.zAdd(key)(MemberScore("v2", 2d))
            newScore <- redis.zAddWithIncr(key)(Increment, MemberScore("v1", 3d))
            result   <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(newScore)(isSome(equalTo(4.0))) && assert(result.toList)(equalTo(List("v2", "v1")))
        }
      ),
      suite("zCard")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(MemberScore("hello", 1d), MemberScore("world", 2d))
            card  <- redis.zCard(key)
          } yield assert(card)(equalTo(2L))
        },
        test("0 when key doesn't exist") {
          assertZIO(ZIO.serviceWithZIO[Redis](_.zCard("unknownSet")))(equalTo(0L))
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            card  <- redis.zCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zCount")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(
                       MemberScore("a", 1d),
                       MemberScore("b", 2d),
                       MemberScore("c", 3d),
                       MemberScore("d", 4d),
                       MemberScore("e", 5d)
                     )
            count <- redis.zCount(key, 0 to 3)
          } yield assert(count)(equalTo(3L))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            count <- redis.zCount(key, 0 to 3)
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zDiff")(
        test("empty sets") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            key3  <- uuid
            diff  <- redis.zDiff(key1, key2, key3).returning[String]
          } yield assert(diff)(isEmpty)
        },
        test("non-empty set with empty set") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(MemberScore("a", 1d), MemberScore("b", 2d))
            diff  <- redis.zDiff(key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("a", "b")))
        },
        test("non-empty sets") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            _     <- redis.zAdd(key2)(MemberScore("a", 1d), MemberScore("b", 2d))
            diff  <- redis.zDiff(key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("c")))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zDiffWithScores")(
        test("empty sets") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            key3  <- uuid
            diff  <- redis.zDiffWithScores(key1, key2, key3).returning[String]
          } yield assert(diff)(isEmpty)
        },
        test("non-empty set with empty set") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(MemberScore("a", 1d), MemberScore("b", 2d))
            diff  <- redis.zDiffWithScores(key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk(MemberScore("a", 1d), MemberScore("b", 2d))))
        },
        test("non-empty sets") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            _     <- redis.zAdd(key2)(MemberScore("a", 1d), MemberScore("b", 2d))
            diff  <- redis.zDiffWithScores(key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk(MemberScore("c", 3d))))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zDiffStore")(
        test("empty sets") {
          for {
            redis <- ZIO.service[Redis]
            dest  <- uuid
            key1  <- uuid
            key2  <- uuid
            key3  <- uuid
            card  <- redis.zDiffStore(dest, key1, key2, key3)
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with empty set") {
          for {
            redis <- ZIO.service[Redis]
            dest  <- uuid
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(
                       MemberScore("a", 1d),
                       MemberScore("b", 2d)
                     )
            card  <- redis.zDiffStore(dest, key1, key2)
          } yield assert(card)(equalTo(2L))
        },
        test("non-empty sets") {
          for {
            redis <- ZIO.service[Redis]
            dest  <- uuid
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(
                       MemberScore("a", 1d),
                       MemberScore("b", 2d),
                       MemberScore("c", 3d)
                     )
            _     <- redis.zAdd(key2)(MemberScore("a", 1d), MemberScore("b", 2d))
            card  <- redis.zDiffStore(dest, key1, key2)
          } yield assert(card)(equalTo(1L))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zIncrBy")(
        test("non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(
                         MemberScore("a", 1d),
                         MemberScore("b", 2d),
                         MemberScore("c", 3d),
                         MemberScore("d", 4d),
                         MemberScore("e", 5d)
                       )
            incrRes <- redis.zIncrBy(key, 10, "a")
            count   <- redis.zCount(key, 10 to 11)
          } yield assert(count)(equalTo(1L)) && assert(incrRes)(equalTo(11.0))
        },
        test("empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            incrRes <- redis.zIncrBy(key, 10, "a")
            count   <- redis.zCount(key, 0 to -1)
          } yield assert(count)(equalTo(0L)) && assert(incrRes)(equalTo(10.0))
        }
      ),
      suite("zInter")(
        test("two non-empty sets") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _       <- redis.zAdd(second)(MemberScore("a", 1d), MemberScore("c", 3d), MemberScore("e", 5d))
            members <- redis.zInter(first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "c")))
        },
        test("empty when one of the sets is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore("a", 1d), MemberScore("b", 2d))
            members  <- redis.zInter(nonEmpty, empty)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("empty when both sets are empty") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            members <- redis.zInter(first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            third   <- uuid
            _       <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _       <- redis.zAdd(second)(MemberScore("b", 2d), MemberScore("b", 2d), MemberScore("d", 4d))
            _       <- redis.zAdd(third)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            members <- redis.zInter(first, second, third)().returning[String]
          } yield assert(members)(equalTo(Chunk("b")))
        },
        test("error when first parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(first, value)
            members <- redis.zInter(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with empty first set and second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(second, value)
            members <- redis.zInter(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with non-empty first set and second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.zAdd(first)(MemberScore("a", 1d))
            _       <- redis.set(second, value)
            members <- redis.zInter(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInter(first, second)(weights = Some(::(2.0, 3.0 :: Nil))).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N")))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInter(first, second)(weights = Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInter(first, second)(weights = Some(::(2.0, List(3.0, 5.0)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInter(first, second)(Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("N", "O")))
        },
        test("set aggregate parameter MIN") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInter(first, second)(Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N")))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zInterWithScores")(
        test("two non-empty sets") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _       <- redis.zAdd(second)(MemberScore("a", 1d), MemberScore("c", 3d), MemberScore("e", 5d))
            members <- redis.zInterWithScores(first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore("a", 2d), MemberScore("c", 6d))))
        },
        test("empty when one of the sets is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore("a", 1d), MemberScore("b", 2d))
            members  <- redis.zInterWithScores(nonEmpty, empty)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("empty when both sets are empty") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            members <- redis.zInterWithScores(first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            third   <- uuid
            _       <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _       <- redis.zAdd(second)(MemberScore("b", 2d), MemberScore("b", 2d), MemberScore("d", 4d))
            _       <- redis.zAdd(third)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            members <- redis.zInterWithScores(first, second, third)().returning[String]
          } yield assert(members)(
            equalTo(Chunk(MemberScore("b", 6d)))
          )
        },
        test("error when first parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(first, value)
            members <- redis.zInterWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with empty first set and second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(second, value)
            members <- redis.zInterWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with non-empty first set and second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.zAdd(first)(MemberScore("a", 1d))
            _       <- redis.set(second, value)
            members <- redis.zInterWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInterWithScores(first, second)(weights = Some(::(2.0, 3.0 :: Nil))).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore("O", 20d), MemberScore("N", 21d))))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInterWithScores(first, second)(weights = Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis
                         .zInterWithScores(first, second)(weights = Some(::(2.0, List(3.0, 5.0))))
                         .returning[String]
                         .either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInterWithScores(first, second)(Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore("N", 6d), MemberScore("O", 7d))))
        },
        test("set aggregate parameter MIN") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zInterWithScores(first, second)(Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore("O", 2d), MemberScore("N", 3d))))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zInterStore")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _      <- redis.zAdd(second)(MemberScore("a", 1d), MemberScore("c", 3d), MemberScore("e", 5d))
            card   <- redis.zInterStore(s"out_$dest", first, second)()
          } yield assert(card)(equalTo(2L))
        },
        test("empty when one of the sets is empty") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore("a", 1d), MemberScore("b", 2d))
            card     <- redis.zInterStore(dest, nonEmpty, empty)()
          } yield assert(card)(equalTo(0L))
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- redis.zInterStore(dest, first, second)()
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _      <- redis.zAdd(second)(MemberScore("b", 2d), MemberScore("b", 2d), MemberScore("d", 4d))
            _      <- redis.zAdd(third)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            card   <- redis.zInterStore(dest, first, second, third)()
          } yield assert(card)(equalTo(1L))
        },
        test("error when first parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(first, value)
            card   <- redis.zInterStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with empty first set and second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(second, value)
            card   <- redis.zInterStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with non-empty first set and second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.zAdd(first)(MemberScore("a", 1d))
            _      <- redis.set(second, value)
            card   <- redis.zInterStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zInterStore(dest, first, second)(weights = Some(::(2.0, 3.0 :: Nil)))
          } yield assert(card)(equalTo(2L))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zInterStore(dest, first, second)(weights = Some(::(2, Nil))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zInterStore(dest, first, second)(weights = Some(::(2.0, List(3.0, 5.0)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zInterStore(dest, first, second)(Some(Aggregate.Max))
          } yield assert(card)(equalTo(2L))
        },
        test("set aggregate parameter MIN") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zInterStore(dest, first, second)(Some(Aggregate.Min))
          } yield assert(card)(equalTo(2L))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zLexCount")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(
                       MemberScore("Delhi", 1d),
                       MemberScore("Mumbai", 2d),
                       MemberScore("London", 3d),
                       MemberScore("Paris", 4d),
                       MemberScore("Tokyo", 5d)
                     )
            count <- redis.zLexCount(key, LexRange(min = LexMinimum.Closed("London"), max = LexMaximum.Open("Paris")))
          } yield assert(count)(equalTo(2L))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            count <- redis.zLexCount(key, LexRange(min = LexMinimum.Closed("London"), max = LexMaximum.Open("Paris")))
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zPopMax")(
        test("non-empty set")(
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            _      <- redis.zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- redis.zPopMax(key).returning[String]
          } yield assert(result.toList)(equalTo(List(tokyo)))
        ),
        test("non-empty set with count param")(
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            _      <- redis.zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- redis.zPopMax(key, Some(3)).returning[String]
          } yield assert(result.toList)(equalTo(List(tokyo, paris, london)))
        ),
        test("empty set")(for {
          redis  <- ZIO.service[Redis]
          key    <- uuid
          result <- redis.zPopMax(key).returning[String]
        } yield assert(result.toList)(isEmpty))
      ),
      suite("zPopMin")(
        test("non-empty set")(
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            _      <- redis.zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- redis.zPopMin(key).returning[String]
          } yield assert(result.toList)(equalTo(List(delhi)))
        ),
        test("non-empty set with count param")(
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            _      <- redis.zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- redis.zPopMin(key, Some(3)).returning[String]
          } yield assert(result.toList)(equalTo(List(delhi, mumbai, london)))
        ),
        test("empty set")(for {
          redis  <- ZIO.service[Redis]
          key    <- uuid
          result <- redis.zPopMin(key).returning[String]
        } yield assert(result.toList)(isEmpty))
      ),
      suite("zRange")(
        test("all members for non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            edge    = MemberScore("The edge of universe", Double.PositiveInfinity)
            quark   = MemberScore("Quark", Double.NegativeInfinity)
            _      <- redis.zAdd(key)(edge, delhi, mumbai, london, tokyo, paris, quark)
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(result.toList)(
            equalTo(
              List("Quark", "Delhi", "Mumbai", "London", "Paris", "Tokyo", "The edge of universe")
            )
          )
        },
        test("some members for non-empty set by inclusive range with negative bounds") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            _      <- redis.zAdd(key)(delhi, mumbai, london, tokyo, paris)
            result <- redis.zRange(key, -3 to -2).returning[String]
          } yield assert(result.toList)(equalTo(List("London", "Paris")))
        },
        test("some members for non-empty set by inclusive range with positive bounds") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            _      <- redis.zAdd(key)(delhi, mumbai, london, tokyo, paris)
            result <- redis.zRange(key, 0 to 2).returning[String]
          } yield assert(result.toList)(equalTo(List("Delhi", "Mumbai", "London")))
        },
        test("some members for non-empty set by exclusive range") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            _      <- redis.zAdd(key)(delhi, mumbai, london, tokyo, paris)
            result <- redis.zRange(key, 1 until -2).returning[String]
          } yield assert(result.toList)(equalTo(List("Mumbai", "London")))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeWithScores")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore("Delhi", 1d)
            mumbai  = MemberScore("Mumbai", 2d)
            london  = MemberScore("London", 3d)
            paris   = MemberScore("Paris", 4d)
            tokyo   = MemberScore("Tokyo", 5d)
            edge    = MemberScore("The edge of universe", Double.PositiveInfinity)
            quark   = MemberScore("Quark", Double.NegativeInfinity)
            _      <- redis.zAdd(key)(edge, delhi, mumbai, quark, london, tokyo, paris)
            result <- redis.zRangeWithScores(key, 0 to -1).returning[String]
          } yield assert(result.toList)(
            equalTo(List(quark, delhi, mumbai, london, paris, tokyo, edge))
          )
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zRangeWithScores(key, 0 to -1).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByLex")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(
                        MemberScore("Delhi", 1d),
                        MemberScore("London", 2d),
                        MemberScore("Paris", 3d),
                        MemberScore("Tokyo", 4d),
                        MemberScore("NewYork", 5d),
                        MemberScore("Seoul", 6d)
                      )
            result <- redis
                        .zRangeByLex(
                          key,
                          LexRange(min = LexMinimum.Open("London"), max = LexMaximum.Closed("Seoul"))
                        )
                        .returning[String]
          } yield assert(result.toList)(equalTo(List("Paris")))
        },
        test("non-empty set with limit") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(
                        MemberScore("Delhi", 1d),
                        MemberScore("London", 2d),
                        MemberScore("Paris", 3d),
                        MemberScore("Tokyo", 4d),
                        MemberScore("NewYork", 5d),
                        MemberScore("Seoul", 6d)
                      )
            result <- redis
                        .zRangeByLex(
                          key,
                          LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded),
                          Some(Limit(2, 3))
                        )
                        .returning[String]
          } yield assert(result.toList)(equalTo(List("Paris", "Tokyo", "NewYork")))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis
                        .zRangeByLex(key, LexRange(min = LexMinimum.Open("A"), max = LexMaximum.Closed("Z")))
                        .returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByScore")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(
                        MemberScore("Samsung", 1556d),
                        MemberScore("Nokia", 2000d),
                        MemberScore("Micromax", 1801d),
                        MemberScore("Sunsui", 2200d),
                        MemberScore("MicroSoft", 1800d),
                        MemberScore("LG", 2500d)
                      )
            result <- redis
                        .zRangeByScore(key, ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)))
                        .returning[String]
          } yield assert(result.toList)(equalTo(List("Samsung", "MicroSoft", "Micromax")))
        },
        test("non-empty set, with limit") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            _         <- redis.zAdd(key)(
                           MemberScore("Samsung", 1556d),
                           MemberScore("Nokia", 2000d),
                           MemberScore("Micromax", 1801d),
                           MemberScore("Sunsui", 2200d),
                           MemberScore("MicroSoft", 1800d),
                           MemberScore("LG", 2500d)
                         )
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(2500))
            result    <- redis.zRangeByScore(key, scoreRange, Some(Limit(offset = 1, count = 3))).returning[String]
          } yield assert(result.toList)(equalTo(List("MicroSoft", "Micromax", "Nokia")))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis
                        .zRangeByScore(key, ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)))
                        .returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByScoreWithScores")(
        test("non-empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            samsung    = MemberScore("Samsung", 1556d)
            nokia      = MemberScore("Nokia", 2000d)
            micromax   = MemberScore("Micromax", 1801d)
            sunsui     = MemberScore("Sunsui", 2200d)
            microSoft  = MemberScore("MicroSoft", 1800d)
            lg         = MemberScore("LG", 2500d)
            _         <- redis.zAdd(key)(samsung, nokia, micromax, sunsui, microSoft, lg)
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900))
            result    <- redis.zRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(result.toList)(equalTo(List(samsung, microSoft, micromax)))
        },
        test("non-empty set, with limit") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            samsung    = MemberScore("Samsung", 1556d)
            nokia      = MemberScore("Nokia", 2000d)
            micromax   = MemberScore("Micromax", 1801d)
            sunsui     = MemberScore("Sunsui", 2200d)
            microSoft  = MemberScore("MicroSoft", 1800d)
            lg         = MemberScore("LG", 2500d)
            _         <- redis.zAdd(key)(samsung, nokia, micromax, sunsui, microSoft, lg)
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(2500))
            result    <-
              redis.zRangeByScoreWithScores(key, scoreRange, Some(Limit(offset = 1, count = 3))).returning[String]
          } yield assert(result.toList)(equalTo(List(microSoft, micromax, nokia)))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900))
            result    <- redis.zRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRank")(
        test("existing elements from non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            rank  <- redis.zRank(key, "c")
          } yield assert(rank)(isSome(equalTo(2L)))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            rank  <- redis.zRank(key, "c")
          } yield assert(rank)(isNone)
        }
      ),
      suite("zRankWithScore")(
        test("existing elements from non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            rank  <- redis.zRankWithScore(key, "c")
          } yield assert(rank)(isSome(equalTo(RankScore(2L, 3d))))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            rank  <- redis.zRankWithScore(key, "c")
          } yield assert(rank)(isNone)
        }
      ),
      suite("zRem")(
        test("existing elements from non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            removed <- redis.zRem(key, "b", "c")
          } yield assert(removed)(equalTo(2L))
        },
        test("when just part of elements are present in the non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            removed <- redis.zRem(key, "b", "d")
          } yield assert(removed)(equalTo(1L))
        },
        test("when none of the elements are present in the non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d))
            removed <- redis.zRem(key, "d", "e")
          } yield assert(removed)(equalTo(0L))
        },
        test("elements from an empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            removed <- redis.zRem(key, "a", "b")
          } yield assert(removed)(equalTo(0L))
        },
        test("elements from not set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            value   <- uuid
            _       <- redis.set(key, value)
            removed <- redis.zRem(key, "a", "b").either
          } yield assert(removed)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zRemRangeByLex")(
        test("non-empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            _           <- redis.zAdd(key)(
                             MemberScore("Delhi", 0d),
                             MemberScore("Mumbai", 0d),
                             MemberScore("Hyderabad", 0d),
                             MemberScore("Kolkata", 0d),
                             MemberScore("Chennai", 0d)
                           )
            remResult   <-
              redis.zRemRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
            rangeResult <- redis
                             .zRangeByLex(key, LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded))
                             .returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Chennai", "Delhi", "Hyderabad"))) &&
            assert(remResult)(equalTo(2L))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            remResult <-
              redis.zRemRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByRank")(
        test("non-empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            _           <- redis.zAdd(key)(
                             MemberScore("Delhi", 1d),
                             MemberScore("Mumbai", 2d),
                             MemberScore("Hyderabad", 3d),
                             MemberScore("Kolkata", 4d),
                             MemberScore("Chennai", 5d)
                           )
            remResult   <- redis.zRemRangeByRank(key, 1 to 2)
            rangeResult <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Delhi", "Kolkata", "Chennai"))) &&
            assert(remResult)(equalTo(2L))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            remResult <- redis.zRemRangeByRank(key, 1 to 2)
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByScore")(
        test("non-empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            _           <- redis.zAdd(key)(
                             MemberScore("Delhi", 80d),
                             MemberScore("Mumbai", 60d),
                             MemberScore("Hyderabad", 70d),
                             MemberScore("Kolkata", 50d),
                             MemberScore("Chennai", 65d)
                           )
            remResult   <-
              redis.zRemRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70)))
            rangeResult <- redis
                             .zRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Infinity))
                             .returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Hyderabad", "Delhi"))) && assert(remResult)(equalTo(3L))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            remResult <-
              redis.zRemRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70)))
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRevRange")(
        test("non-empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            _         <- redis.zAdd(key)(
                           MemberScore("Delhi", 80d),
                           MemberScore("Mumbai", 60d),
                           MemberScore("Hyderabad", 70d),
                           MemberScore("Kolkata", 50d),
                           MemberScore("Chennai", 65d)
                         )
            revResult <- redis.zRevRange(key, 0 to 1).returning[String]
          } yield assert(revResult.toList)(equalTo(List("Delhi", "Hyderabad")))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            remResult <- redis.zRevRange(key, 0 to -1).returning[String]
          } yield assert(remResult.toList)(isEmpty)
        }
      ),
      suite("zRevRangeWithScores")(
        test("non-empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            delhi      = MemberScore("Delhi", 80d)
            mumbai     = MemberScore("Mumbai", 60d)
            hyderabad  = MemberScore("Hyderabad", 70d)
            kolkata    = MemberScore("Kolkata", 50d)
            chennai    = MemberScore("Chennai", 65d)
            _         <- redis.zAdd(key)(delhi, mumbai, hyderabad, kolkata, chennai)
            revResult <- redis.zRevRangeWithScores(key, 0 to 1).returning[String]
          } yield assert(revResult.toList)(equalTo(List(delhi, hyderabad)))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            remResult <- redis.zRevRange(key, 0 to -1).returning[String]
          } yield assert(remResult.toList)(isEmpty)
        }
      ),
      suite("zRevRangeByLex")(
        test("non-empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            _           <- redis.zAdd(key)(
                             MemberScore("Delhi", 0d),
                             MemberScore("London", 0d),
                             MemberScore("Paris", 0d),
                             MemberScore("Tokyo", 0d),
                             MemberScore("NewYork", 0d),
                             MemberScore("Seoul", 0d)
                           )
            lexRange     = LexRange(min = LexMinimum.Closed("Delhi"), max = LexMaximum.Open("Seoul"))
            rangeResult <- redis.zRevRangeByLex(key, lexRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Paris", "NewYork", "London", "Delhi")))
        },
        test("non-empty set with limit") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            _           <- redis.zAdd(key)(
                             MemberScore("Delhi", 0d),
                             MemberScore("London", 0d),
                             MemberScore("Paris", 0d),
                             MemberScore("Tokyo", 0d),
                             MemberScore("NewYork", 0d),
                             MemberScore("Seoul", 0d)
                           )
            lexRange     = LexRange(min = LexMinimum.Closed("Delhi"), max = LexMaximum.Open("Seoul"))
            rangeResult <- redis.zRevRangeByLex(key, lexRange, Some(Limit(offset = 1, count = 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("NewYork", "London")))
        },
        test("empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            lexRange     = LexRange(min = LexMinimum.Closed("Mumbai"), max = LexMaximum.Open("Hyderabad"))
            rangeResult <- redis.zRevRangeByLex(key, lexRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRangeByScore")(
        test("non-empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            _           <- redis.zAdd(key)(
                             MemberScore("Samsung", 1556d),
                             MemberScore("Nokia", 2000d),
                             MemberScore("Micromax", 1800d),
                             MemberScore("Sunsui", 2200d),
                             MemberScore("MicroSoft", 1800d),
                             MemberScore("LG", 2500d)
                           )
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScore(key, scoreRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Sunsui", "Nokia")))
        },
        test("non-empty set with limit") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            _           <- redis.zAdd(key)(
                             MemberScore("Samsung", 1556d),
                             MemberScore("Nokia", 2000d),
                             MemberScore("Micromax", 1800d),
                             MemberScore("Sunsui", 2200d),
                             MemberScore("MicroSoft", 1800d),
                             MemberScore("LG", 2500d)
                           )
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScore(key, scoreRange, Some(Limit(1, 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Nokia")))
        },
        test("empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScore(key, scoreRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRangeByScoreWithScores")(
        test("non-empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            samsung      = MemberScore("Samsung", 1556d)
            nokia        = MemberScore("Nokia", 2000d)
            micromax     = MemberScore("Micromax", 1800d)
            sunsui       = MemberScore("Sunsui", 2200d)
            nicroSoft    = MemberScore("MicroSoft", 1800d)
            lg           = MemberScore("LG", 2500d)
            _           <- redis.zAdd(key)(samsung, nokia, micromax, sunsui, nicroSoft, lg)
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List(sunsui, nokia)))
        },
        test("non-empty set with limit") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            samsung      = MemberScore("Samsung", 1556d)
            nokia        = MemberScore("Nokia", 2000d)
            micromax     = MemberScore("Micromax", 1800d)
            sunsui       = MemberScore("Sunsui", 2200d)
            nicroSoft    = MemberScore("MicroSoft", 1800d)
            lg           = MemberScore("LG", 2500d)
            _           <- redis.zAdd(key)(samsung, nokia, micromax, sunsui, nicroSoft, lg)
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScoreWithScores(key, scoreRange, Some(Limit(1, 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List(nokia)))
        },
        test("empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRank")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(
                        MemberScore("Delhi", 10d),
                        MemberScore("Mumbai", 20d),
                        MemberScore("Hyderabad", 30d),
                        MemberScore("Kolkata", 40d),
                        MemberScore("Chennai", 50d)
                      )
            result <- redis.zRevRank(key, "Hyderabad")
          } yield assert(result)(isSome(equalTo(2L)))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zRevRank(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zRevRankWithScore")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(
                        MemberScore("Delhi", 10d),
                        MemberScore("Mumbai", 20d),
                        MemberScore("Hyderabad", 30d),
                        MemberScore("Kolkata", 40d),
                        MemberScore("Chennai", 50d)
                      )
            result <- redis.zRevRankWithScore(key, "Kolkata")
          } yield assert(result)(isSome(equalTo(RankScore(1L, 40d))))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zRevRankWithScore(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zScan")(
        test("non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            a        = MemberScore("atest", 1d)
            b        = MemberScore("btest", 2d)
            c        = MemberScore("ctest", 3d)
            _       <- redis.zAdd(key)(a, b, c)
            members <- scanAll(key)
          } yield assert(members)(equalTo(Chunk(a, b, c)))
        },
        test("with infinity in set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            a        = MemberScore("a", 1d)
            b        = MemberScore("b", 2d)
            inf      = MemberScore("inf", Double.PositiveInfinity)
            negInf   = MemberScore("neg inf", Double.NegativeInfinity)
            _       <- redis.zAdd(key)(a, b, inf, negInf)
            members <- scanAll(key)
          } yield assert(members)(equalTo(Chunk(negInf, a, b, inf)))
        },
        test("empty set") {
          for {
            redis            <- ZIO.service[Redis]
            key              <- uuid
            scan             <- redis.zScan(key, 0L).returning[String]
            (cursor, members) = scan
          } yield assert(cursor)(isZero) && assert(members)(isEmpty)
        },
        test("with match over non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            one      = MemberScore("one", 1d)
            two      = MemberScore("two", 2d)
            three    = MemberScore("three", 3d)
            _       <- redis.zAdd(key)(one, two, three)
            members <- scanAll(key, Some("t[a-z]*"))
          } yield assert(members)(equalTo(Chunk(two, three)))
        },
        test("with count over non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(
                         MemberScore("a", 1d),
                         MemberScore("b", 2d),
                         MemberScore("c", 3d),
                         MemberScore("d", 4d),
                         MemberScore("e", 5d)
                       )
            members <- scanAll(key, count = Some(Count(3L)))
          } yield assert(members)(isNonEmpty)
        },
        test("match with count over non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(
                         MemberScore("testa", 1d),
                         MemberScore("testb", 2d),
                         MemberScore("testc", 3d),
                         MemberScore("testd", 4d),
                         MemberScore("teste", 5d)
                       )
            members <- scanAll(key, pattern = Some("t[a-z]*"), count = Some(Count(3L)))
          } yield assert(members)(isNonEmpty)
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            scan  <- redis.zScan(key, 0L).returning[String].either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zScore")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(
                        MemberScore("Delhi", 10d),
                        MemberScore("Mumbai", 20d),
                        MemberScore("Hyderabad", 30d),
                        MemberScore("Kolkata", 40d),
                        MemberScore("Chennai", 50d)
                      )
            result <- redis.zScore(key, "Delhi")
          } yield assert(result)(isSome(equalTo(10.0)))
        },
        test("infinity score in set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore("Delhi", 10d), MemberScore("Infinity", Double.PositiveInfinity))
            result <- redis.zScore(key, "Infinity")
          } yield assert(result)(isSome(equalTo(Double.PositiveInfinity)))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zScore(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zMScore")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(
                        MemberScore("Delhi", 10d),
                        MemberScore("Mumbai", 20d),
                        MemberScore("Hyderabad", 30d),
                        MemberScore("Kolkata", 40d),
                        MemberScore("Chennai", 50d)
                      )
            result <- redis.zMScore(key, "Delhi", "Mumbai", "notFound")
          } yield assert(result)(equalTo(Chunk(Some(10d), Some(20d), None)))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zMScore(key, "Hyderabad")
          } yield assert(result)(equalTo(Chunk(None)))
        },
        test("infinity score") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(
                        MemberScore("Delhi", 10d),
                        MemberScore("Infinity", Double.PositiveInfinity),
                        MemberScore("-Infinity", Double.NegativeInfinity)
                      )
            result <- redis.zMScore(key, "Infinity", "-Infinity", "Delhi", "Ankh-Morpork")
          } yield assert(result)(
            equalTo(
              Chunk(
                Some(Double.PositiveInfinity),
                Some(Double.NegativeInfinity),
                Some(10d),
                None
              )
            )
          )
        }
      ),
      suite("zUnion")(
        test("two non-empty sets") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _       <- redis.zAdd(second)(MemberScore("a", 1d), MemberScore("c", 3d), MemberScore("e", 5d))
            members <- redis.zUnion(first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "b", "d", "e", "c")))
        },
        test("equal to the non-empty set when the other one is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore("a", 1d), MemberScore("b", 2d))
            members  <- redis.zUnion(nonEmpty, empty)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "b")))
        },
        test("empty when both sets are empty") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            members <- redis.zUnion(first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            third   <- uuid
            _       <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _       <- redis.zAdd(second)(MemberScore("b", 2d), MemberScore("d", 4d))
            _       <- redis.zAdd(third)(MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("e", 5d))
            members <- redis.zUnion(first, second, third)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "e", "b", "c", "d")))
        },
        test("error when the first parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(first, value)
            members <- redis.zUnion(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when the first parameter is set and the second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.zAdd(first)(MemberScore("a", 1d))
            _       <- redis.set(second, value)
            members <- redis.zUnion(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnion(first, second)(Some(::(2, List(3)))).returning[String]
          } yield assert(members)(equalTo(Chunk("M", "P", "O", "N")))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnion(first, second)(Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnion(first, second)(Some(::(2, List(3, 5)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnion(first, second)(aggregate = Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("P", "M", "N", "O")))
        },
        test("set aggregate parameter MIN") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnion(first, second)(aggregate = Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N", "P", "M")))
        },
        test("parameter weights provided along with aggregate") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnion(first, second)(Some(::(2, List(3))), Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("M", "N", "P", "O")))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zUnionWithScores")(
        test("two non-empty sets") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _       <- redis.zAdd(second)(MemberScore("a", 1d), MemberScore("c", 3d), MemberScore("e", 5d))
            members <- redis.zUnionWithScores(first, second)().returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore("a", 2d),
                MemberScore("b", 2d),
                MemberScore("d", 4d),
                MemberScore("e", 5d),
                MemberScore("c", 6d)
              )
            )
          )
        },
        test("equal to the non-empty set when the other one is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore("a", 1d), MemberScore("b", 2d))
            members  <- redis.zUnionWithScores(nonEmpty, empty)().returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore("a", 1d), MemberScore("b", 2d))))
        },
        test("empty when both sets are empty") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            members <- redis.zUnionWithScores(first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            third   <- uuid
            _       <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _       <- redis.zAdd(second)(MemberScore("b", 2d), MemberScore("d", 4d))
            _       <- redis.zAdd(third)(MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("e", 5d))
            members <- redis.zUnionWithScores(first, second, third)().returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore("a", 1d),
                MemberScore("e", 5d),
                MemberScore("b", 6d),
                MemberScore("c", 6d),
                MemberScore("d", 8d)
              )
            )
          )
        },
        test("error when the first parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(first, value)
            members <- redis.zUnionWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when the first parameter is set and the second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.zAdd(first)(MemberScore("a", 1d))
            _       <- redis.set(second, value)
            members <- redis.zUnionWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnionWithScores(first, second)(Some(::(2, List(3)))).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore("M", 10d),
                MemberScore("P", 12d),
                MemberScore("O", 20d),
                MemberScore("N", 21d)
              )
            )
          )
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnionWithScores(first, second)(Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnionWithScores(first, second)(Some(::(2, List(3, 5)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnionWithScores(first, second)(aggregate = Some(Aggregate.Max)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore("P", 4d),
                MemberScore("M", 5d),
                MemberScore("N", 6d),
                MemberScore("O", 7d)
              )
            )
          )
        },
        test("set aggregate parameter MIN") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <- redis.zUnionWithScores(first, second)(aggregate = Some(Aggregate.Min)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore("O", 2d),
                MemberScore("N", 3d),
                MemberScore("P", 4d),
                MemberScore("M", 5d)
              )
            )
          )
        },
        test("parameter weights provided along with aggregate") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _       <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            members <-
              redis.zUnionWithScores(first, second)(Some(::(2, List(3))), Some(Aggregate.Max)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore("M", 10d),
                MemberScore("N", 12d),
                MemberScore("P", 12d),
                MemberScore("O", 14d)
              )
            )
          )
        }
      ) @@ clusterExecutorUnsupported,
      suite("zUnionStore")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _      <- redis.zAdd(second)(MemberScore("a", 1d), MemberScore("c", 3d), MemberScore("e", 5d))
            card   <- redis.zUnionStore(dest, first, second)()
          } yield assert(card)(equalTo(5L))
        },
        test("equal to the non-empty set when the other one is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            dest     <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore("a", 1d), MemberScore("b", 2d))
            card     <- redis.zUnionStore(dest, nonEmpty, empty)()
          } yield assert(card)(equalTo(2L))
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            card   <- redis.zUnionStore(dest, first, second)()
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            dest   <- uuid
            _      <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            _      <- redis.zAdd(second)(MemberScore("b", 2d), MemberScore("d", 4d))
            _      <- redis.zAdd(third)(MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("e", 5d))
            card   <- redis.zUnionStore(dest, first, second, third)()
          } yield assert(card)(equalTo(5L))
        },
        test("error when the first parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- redis.set(first, value)
            card   <- redis.zUnionStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when the first parameter is set and the second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- redis.zAdd(first)(MemberScore("a", 1d))
            _      <- redis.set(second, value)
            card   <- redis.zUnionStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zUnionStore(dest, first, second)(Some(::(2, List(3))))
          } yield assert(card)(equalTo(4L))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zUnionStore(dest, first, second)(Some(::(2, Nil))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zUnionStore(dest, first, second)(Some(::(2, List(3, 5)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zUnionStore(dest, first, second)(aggregate = Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        },
        test("set aggregate parameter MIN") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zUnionStore(dest, first, second)(aggregate = Some(Aggregate.Min))
          } yield assert(card)(equalTo(4L))
        },
        test("parameter weights provided along with aggregate") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore("M", 5d), MemberScore("N", 6d), MemberScore("O", 7d))
            _      <- redis.zAdd(second)(MemberScore("N", 3d), MemberScore("O", 2d), MemberScore("P", 4d))
            card   <- redis.zUnionStore(dest, first, second)(Some(::(2, List(3))), Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zRandMember")(
        test("key does not exist") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            notExists <- uuid
            _         <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            ret       <- redis.zRandMember(notExists).returning[String]
          } yield assert(ret)(isNone)
        },
        test("key does not exist with count") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            notExists <- uuid
            _         <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            ret       <- redis.zRandMember(notExists, 1).returning[String]
          } yield assert(ret)(isEmpty)
        },
        test("get an element") {
          for {
            redis <- ZIO.service[Redis]
            first <- uuid
            _     <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            ret   <- redis.zRandMember(first).returning[String]
          } yield assert(ret)(isSome)
        },
        test("get elements with count") {
          for {
            redis <- ZIO.service[Redis]
            first <- uuid
            _     <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            ret   <- redis.zRandMember(first, 2).returning[String]
          } yield assert(ret)(hasSize(equalTo(2)))
        }
      ),
      suite("zRandMemberWithScores")(
        test("key does not exist") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            notExists <- uuid
            _         <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            ret       <- redis.zRandMemberWithScores(notExists, 1).returning[String]
          } yield assert(ret)(isEmpty)
        },
        test("get elements with count") {
          for {
            redis <- ZIO.service[Redis]
            first <- uuid
            _     <-
              redis.zAdd(first)(MemberScore("a", 1d), MemberScore("b", 2d), MemberScore("c", 3d), MemberScore("d", 4d))
            ret   <- redis.zRandMemberWithScores(first, 2).returning[String]
          } yield assert(ret)(hasSize(equalTo(2)))
        }
      )
    )

  private def scanAll(
    key: String,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ZIO[Redis, RedisError, Chunk[MemberScore[String]]] =
    ZStream
      .paginateChunkZIO(0L) { cursor =>
        ZIO.serviceWithZIO[Redis](_.zScan(key, cursor, pattern, count).returning[String]).map {
          case (nc, nm) if nc == 0 => (nm, None)
          case (nc, nm)            => (nm, Some(nc))
        }
      }
      .runCollect

}
