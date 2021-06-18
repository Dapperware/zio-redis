package zio.redis

import zio.duration.Duration
import zio.redis.RedisError.{ ProtocolError, WrongType }
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, ZIO }

trait SortedSetsSpec extends BaseSpec {
  val sortedSetsSuite: Spec[RedisExecutor, TestFailure[RedisError], TestSuccess] =
    suite("sorted sets")(
      suite("bzPopMax")(
        testM("non-empty set")(
          for {
            key1    <- uuid
            key2    <- uuid
            key3    <- uuid
            duration = Duration.fromMillis(1000)
            delhi    = MemberScore(1d, "Delhi")
            mumbai   = MemberScore(2d, "Mumbai")
            london   = MemberScore(3d, "London")
            paris    = MemberScore(4d, "Paris")
            tokyo    = MemberScore(5d, "Tokyo")
            _       <- zAdd(key1)(delhi, tokyo)
            _       <- zAdd(key2)(mumbai, paris)
            _       <- zAdd(key3)(london)
            result  <- bzPopMax[String, String](duration, key1, key2, key3)
          } yield assert(result)(isSome(equalTo((key1, tokyo))))
        ),
        testM("empty set")(
          for {
            key     <- uuid
            duration = Duration.fromMillis(1000)
            result  <- bzPopMax[String, String](duration, key)
          } yield assert(result)(isNone)
        )
      ),
      suite("bzPopMin")(
        testM("non-empty set")(
          for {
            key1    <- uuid
            key2    <- uuid
            key3    <- uuid
            duration = Duration.fromMillis(1000)
            delhi    = MemberScore(1d, "Delhi")
            london   = MemberScore(3d, "London")
            paris    = MemberScore(4d, "Paris")
            _       <- zAdd(key2)(delhi, paris)
            _       <- zAdd(key3)(london)
            result  <- bzPopMin[String, String](duration, key1, key2, key3)
          } yield assert(result)(isSome(equalTo((key2, delhi))))
        ),
        testM("empty set")(
          for {
            key     <- uuid
            duration = Duration.fromMillis(1000)
            result  <- bzPopMin[String, String](duration, key)
          } yield assert(result)(isNone)
        )
      ),
      suite("zAdd")(
        testM("to empty set") {
          for {
            key   <- uuid
            value <- uuid
            added <- zAdd(key)(MemberScore(1d, value))
          } yield assert(added)(equalTo(1L))
        },
        testM("to the non-empty set") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- zAdd(key)(MemberScore(1d, value))
            value2 <- uuid
            added  <- zAdd(key)(MemberScore(2d, value2))
          } yield assert(added)(equalTo(1L))
        },
        testM("existing element to set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- zAdd(key)(MemberScore(1d, value))
            added <- zAdd(key)(MemberScore(2d, value))
          } yield assert(added)(equalTo(0L))
        },
        testM("multiple elements to set") {
          for {
            key   <- uuid
            added <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
          } yield assert(added)(equalTo(3L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            added <- zAdd(key)(MemberScore(1d, value)).either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("NX - do not to update existing members, only add new") {
          for {
            key    <- uuid
            _      <- zAdd(key)(MemberScore(1d, "v1"))
            _      <- zAdd(key)(MemberScore(2d, "v2"))
            added  <- zAdd(key, Some(Update.SetNew))(MemberScore(3d, "v3"), MemberScore(22d, "v2"))
            result <- zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v1", "v2", "v3")))
        },
        testM("XX - update existing members, not add new") {
          for {
            key    <- uuid
            _      <- zAdd(key)(MemberScore(1d, "v1"))
            _      <- zAdd(key)(MemberScore(2d, "v2"))
            added  <- zAdd(key, Some(Update.SetExisting))(MemberScore(3d, "v3"), MemberScore(11d, "v1"))
            result <- zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(0L)) && assert(result.toList)(equalTo(List("v2", "v1")))
        },
        testM("CH - return number of new and updated members") {
          for {
            key    <- uuid
            _      <- zAdd(key)(MemberScore(1d, "v1"))
            _      <- zAdd(key)(MemberScore(2d, "v2"))
            added  <- zAdd(key, change = Some(Changed))(MemberScore(3d, "v3"), MemberScore(11d, "v1"))
            result <- zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(2L)) && assert(result.toList)(equalTo(List("v2", "v3", "v1")))
        },
        testM("LT - return number of new members") {
          for {
            key    <- uuid
            _      <- zAdd(key)(MemberScore(3d, "v1"))
            _      <- zAdd(key)(MemberScore(4d, "v2"))
            added  <- zAdd(key, update = Some(Update.SetLessThan))(MemberScore(1d, "v3"), MemberScore(2d, "v1"))
            result <- zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v3", "v1", "v2")))
        },
        testM("GT - return number of new members") {
          for {
            key    <- uuid
            _      <- zAdd(key)(MemberScore(1d, "v1"))
            _      <- zAdd(key)(MemberScore(2d, "v2"))
            added  <- zAdd(key, update = Some(Update.SetGreaterThan))(MemberScore(1d, "v3"), MemberScore(3d, "v1"))
            result <- zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v3", "v2", "v1")))
        },
        testM("GT CH - return number of new and updated members") {
          for {
            key <- uuid
            _   <- zAdd(key)(MemberScore(1d, "v1"))
            _   <- zAdd(key)(MemberScore(2d, "v2"))
            added <- zAdd(key, update = Some(Update.SetGreaterThan), change = Some(Changed))(
                       MemberScore(1d, "v3"),
                       MemberScore(3d, "v1")
                     )
            result <- zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(2L)) && assert(result.toList)(equalTo(List("v3", "v2", "v1")))
        },
        testM("INCR - increment by score") {
          for {
            key      <- uuid
            _        <- zAdd(key)(MemberScore(1d, "v1"))
            _        <- zAdd(key)(MemberScore(2d, "v2"))
            newScore <- zAddWithIncr(key)(Increment, MemberScore(3d, "v1"))
            result   <- zRange(key, 0 to -1).returning[String]
          } yield assert(newScore)(equalTo(Some(4.0))) && assert(result.toList)(equalTo(List("v2", "v1")))
        }
      ),
      suite("zCard")(
        testM("non-empty set") {
          for {
            key  <- uuid
            _    <- zAdd(key)(MemberScore(1d, "hello"), MemberScore(2d, "world"))
            card <- zCard(key)
          } yield assert(card)(equalTo(2L))
        },
        testM("0 when key doesn't exist") {
          assertM(zCard("unknownSet"))(equalTo(0L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            card  <- zCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zCount")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            count <- zCount(key, 0 to 3)
          } yield assert(count)(equalTo(3L))
        },
        testM("empty set") {
          for {
            key   <- uuid
            count <- zCount(key, 0 to 3)
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zDiff")(
        testM("empty sets") {
          for {
            key1 <- uuid
            key2 <- uuid
            key3 <- uuid
            diff <- zDiff(3, key1, key2, key3).returning[String]
          } yield assert(diff)(isEmpty)
        },
        testM("non-empty set with empty set") {
          for {
            key1 <- uuid
            key2 <- uuid
            _    <- zAdd(key1)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            diff <- zDiff(2, key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("a", "b")))
        },
        testM("non-empty sets") {
          for {
            key1 <- uuid
            key2 <- uuid
            _    <- zAdd(key1)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            _    <- zAdd(key2)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            diff <- zDiff(2, key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("c")))
        }
      ),
      suite("zDiffWithScores")(
        testM("empty sets") {
          for {
            key1 <- uuid
            key2 <- uuid
            key3 <- uuid
            diff <- zDiffWithScores(3, key1, key2, key3).returning[String]
          } yield assert(diff)(isEmpty)
        },
        testM("non-empty set with empty set") {
          for {
            key1 <- uuid
            key2 <- uuid
            _    <- zAdd(key1)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            diff <- zDiffWithScores(2, key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk(MemberScore(1d, "a"), MemberScore(2d, "b"))))
        },
        testM("non-empty sets") {
          for {
            key1 <- uuid
            key2 <- uuid
            _    <- zAdd(key1)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            _    <- zAdd(key2)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            diff <- zDiffWithScores(2, key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk(MemberScore(3d, "c"))))
        }
      ),
      suite("zDiffStore")(
        testM("empty sets") {
          for {
            dest <- uuid
            key1 <- uuid
            key2 <- uuid
            key3 <- uuid
            card <- zDiffStore(dest, 3, key1, key2, key3)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with empty set") {
          for {
            dest <- uuid
            key1 <- uuid
            key2 <- uuid
            _ <- zAdd(key1)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b")
                 )
            card <- zDiffStore(dest, 2, key1, key2)
          } yield assert(card)(equalTo(2L))
        },
        testM("non-empty sets") {
          for {
            dest <- uuid
            key1 <- uuid
            key2 <- uuid
            _ <- zAdd(key1)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c")
                 )
            _    <- zAdd(key2)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            card <- zDiffStore(dest, 2, key1, key2)
          } yield assert(card)(equalTo(1L))
        }
      ),
      suite("zIncrBy")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            incrRes <- zIncrBy(key, 10, "a")
            count   <- zCount(key, 10 to 11)
          } yield assert(count)(equalTo(1L)) && assert(incrRes)(equalTo(11.0))
        },
        testM("empty set") {
          for {
            key     <- uuid
            incrRes <- zIncrBy(key, 10, "a")
            count   <- zCount(key, 0 to -1)
          } yield assert(count)(equalTo(0L)) && assert(incrRes)(equalTo(10.0))
        }
      ),
      suite("zInter")(
        testM("two non-empty sets") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- zInter(2, first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "c")))
        },
        testM("empty when one of the sets is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            members  <- zInter(2, nonEmpty, empty)().returning[String]
          } yield assert(members)(isEmpty)
        },
        testM("empty when both sets are empty") {
          for {
            first   <- uuid
            second  <- uuid
            members <- zInter(2, first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first   <- uuid
            second  <- uuid
            third   <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- zAdd(second)(MemberScore(2d, "b"), MemberScore(2d, "b"), MemberScore(4d, "d"))
            _       <- zAdd(third)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            members <- zInter(3, first, second, third)().returning[String]
          } yield assert(members)(
            equalTo(Chunk("b"))
          )
        },
        testM("error when first parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- set(first, value)
            members <- zInter(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with empty first set and second parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- set(second, value)
            members <- zInter(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"))
            _       <- set(second, value)
            members <- zInter(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInter(2, first, second)(weights = Some(::(2.0, 3.0 :: Nil))).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N")))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInter(2, first, second)(weights = Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInter(2, first, second)(weights = Some(::(2.0, List(3.0, 5.0)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInter(2, first, second)(Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("N", "O")))
        },
        testM("set aggregate parameter MIN") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInter(2, first, second)(Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N")))
        }
      ),
      suite("zInterWithScores")(
        testM("two non-empty sets") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- zInterWithScores(2, first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(2d, "a"), MemberScore(6d, "c"))))
        },
        testM("empty when one of the sets is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            members  <- zInterWithScores(2, nonEmpty, empty)().returning[String]
          } yield assert(members)(isEmpty)
        },
        testM("empty when both sets are empty") {
          for {
            first   <- uuid
            second  <- uuid
            members <- zInterWithScores(2, first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first   <- uuid
            second  <- uuid
            third   <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- zAdd(second)(MemberScore(2d, "b"), MemberScore(2d, "b"), MemberScore(4d, "d"))
            _       <- zAdd(third)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            members <- zInterWithScores(3, first, second, third)().returning[String]
          } yield assert(members)(
            equalTo(Chunk(MemberScore(6d, "b")))
          )
        },
        testM("error when first parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- set(first, value)
            members <- zInterWithScores(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with empty first set and second parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- set(second, value)
            members <- zInterWithScores(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"))
            _       <- set(second, value)
            members <- zInterWithScores(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInterWithScores(2, first, second)(weights = Some(::(2.0, 3.0 :: Nil))).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(20d, "O"), MemberScore(21d, "N"))))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInterWithScores(2, first, second)(weights = Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInterWithScores(2, first, second)(weights = Some(::(2.0, List(3.0, 5.0))))
                         .returning[String]
                         .either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInterWithScores(2, first, second)(Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(6d, "N"), MemberScore(7d, "O"))))
        },
        testM("set aggregate parameter MIN") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zInterWithScores(2, first, second)(Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(2d, "O"), MemberScore(3d, "N"))))
        }
      ),
      suite("zInterStore")(
        testM("two non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _      <- zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card   <- zInterStore(s"out_$dest", 2, first, second)()
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when one of the sets is empty") {
          for {
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            card     <- zInterStore(dest, 2, nonEmpty, empty)()
          } yield assert(card)(equalTo(0L))
        },
        testM("empty when both sets are empty") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- zInterStore(dest, 2, first, second)()
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _      <- zAdd(second)(MemberScore(2d, "b"), MemberScore(2d, "b"), MemberScore(4d, "d"))
            _      <- zAdd(third)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            card   <- zInterStore(dest, 3, first, second, third)()
          } yield assert(card)(equalTo(1L))
        },
        testM("error when first parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value)
            card   <- zInterStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value)
            card   <- zInterStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"))
            _      <- set(second, value)
            card   <- zInterStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(weights = Some(::(2.0, 3.0 :: Nil)))
          } yield assert(card)(equalTo(2L))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(weights = Some(::(2, Nil))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(weights = Some(::(2.0, List(3.0, 5.0)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(Some(Aggregate.Max))
          } yield assert(card)(equalTo(2L))
        },
        testM("set aggregate parameter MIN") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(Some(Aggregate.Min))
          } yield assert(card)(equalTo(2L))
        }
      ),
      suite("zLexCount")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
                 )
            count <- zLexCount(key, LexRange(min = LexMinimum.Closed("London"), max = LexMaximum.Open("Paris")))
          } yield assert(count)(equalTo(2L))
        },
        testM("empty set") {
          for {
            key   <- uuid
            count <- zLexCount(key, LexRange(min = LexMinimum.Closed("London"), max = LexMaximum.Open("Paris")))
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zPopMax")(
        testM("non-empty set")(
          for {
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- zPopMax(key).returning[String]
          } yield assert(result.toList)(equalTo(List(tokyo)))
        ),
        testM("non-empty set with count param")(
          for {
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- zPopMax(key, Some(3)).returning[String]
          } yield assert(result.toList)(equalTo(List(tokyo, paris, london)))
        ),
        testM("empty set")(for {
          key    <- uuid
          result <- zPopMax(key).returning[String]
        } yield assert(result.toList)(equalTo(Nil)))
      ),
      suite("zPopMin")(
        testM("non-empty set")(
          for {
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- zPopMin(key).returning[String]
          } yield assert(result.toList)(equalTo(List(delhi)))
        ),
        testM("non-empty set with count param")(
          for {
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- zPopMin(key, Some(3)).returning[String]
          } yield assert(result.toList)(equalTo(List(delhi, mumbai, london)))
        ),
        testM("empty set")(for {
          key    <- uuid
          result <- zPopMin(key).returning[String]
        } yield assert(result.toList)(equalTo(Nil)))
      ),
      suite("zRange")(
        testM("non-empty set") {
          for {
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- zAdd(key)(delhi, mumbai, london, tokyo, paris)
            result <- zRange(key, 0 to -1).returning[String]
          } yield assert(result.toList)(equalTo(List("Delhi", "Mumbai", "London", "Paris", "Tokyo")))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRange(key, 0 to -1).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeWithScores")(
        testM("non-empty set") {
          for {
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- zAdd(key)(delhi, mumbai, london, tokyo, paris)
            result <- zRangeWithScores(key, 0 to -1).returning[String]
          } yield assert(result.toList)(
            equalTo(List(delhi, mumbai, london, paris, tokyo))
          )
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRangeWithScores(key, 0 to -1).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByLex")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "London"),
                   MemberScore(3d, "Paris"),
                   MemberScore(4d, "Tokyo"),
                   MemberScore(5d, "NewYork"),
                   MemberScore(6d, "Seoul")
                 )
            result <- zRangeByLex(
                        key,
                        LexRange(min = LexMinimum.Open("London"), max = LexMaximum.Closed("Seoul"))
                      ).returning[String]
          } yield assert(result.toList)(equalTo(List("Paris")))
        },
        testM("non-empty set with limit") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "London"),
                   MemberScore(3d, "Paris"),
                   MemberScore(4d, "Tokyo"),
                   MemberScore(5d, "NewYork"),
                   MemberScore(6d, "Seoul")
                 )
            result <- zRangeByLex(
                        key,
                        LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded),
                        Some(Limit(2, 3))
                      ).returning[String]
          } yield assert(result.toList)(equalTo(List("Paris", "Tokyo", "NewYork")))
        },
        testM("empty set") {
          for {
            key <- uuid
            result <- zRangeByLex(key, LexRange(min = LexMinimum.Open("A"), max = LexMaximum.Closed("Z")))
                        .returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByScore")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1801d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            result <- zRangeByScore(key, ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)))
                        .returning[String]
          } yield assert(result.toList)(equalTo(List("Samsung", "MicroSoft", "Micromax")))
        },
        testM("non-empty set, with limit") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1801d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(2500))
            result    <- zRangeByScore(key, scoreRange, Some(Limit(offset = 1, count = 3))).returning[String]
          } yield assert(result.toList)(equalTo(List("MicroSoft", "Micromax", "Nokia")))
        },
        testM("empty set") {
          for {
            key <- uuid
            result <- zRangeByScore(key, ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)))
                        .returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByScoreWithScores")(
        testM("non-empty set") {
          for {
            key       <- uuid
            samsung    = MemberScore(1556d, "Samsung")
            nokia      = MemberScore(2000d, "Nokia")
            micromax   = MemberScore(1801d, "Micromax")
            sunsui     = MemberScore(2200d, "Sunsui")
            microSoft  = MemberScore(1800d, "MicroSoft")
            lg         = MemberScore(2500d, "LG")
            _         <- zAdd(key)(samsung, nokia, micromax, sunsui, microSoft, lg)
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900))
            result    <- zRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(result.toList)(equalTo(List(samsung, microSoft, micromax)))
        },
        testM("non-empty set, with limit") {
          for {
            key       <- uuid
            samsung    = MemberScore(1556d, "Samsung")
            nokia      = MemberScore(2000d, "Nokia")
            micromax   = MemberScore(1801d, "Micromax")
            sunsui     = MemberScore(2200d, "Sunsui")
            microSoft  = MemberScore(1800d, "MicroSoft")
            lg         = MemberScore(2500d, "LG")
            _         <- zAdd(key)(samsung, nokia, micromax, sunsui, microSoft, lg)
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(2500))
            result    <- zRangeByScoreWithScores(key, scoreRange, Some(Limit(offset = 1, count = 3))).returning[String]
          } yield assert(result.toList)(equalTo(List(microSoft, micromax, nokia)))
        },
        testM("empty set") {
          for {
            key       <- uuid
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900))
            result    <- zRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRank")(
        testM("existing elements from non-empty set") {
          for {
            key  <- uuid
            _    <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            rank <- zRank(key, "c")
          } yield assert(rank)(equalTo(Some(2L)))
        },
        testM("empty set") {
          for {
            key  <- uuid
            rank <- zRank(key, "c")
          } yield assert(rank)(isNone)
        }
      ),
      suite("zRem")(
        testM("existing elements from non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- zRem(key, "b", "c")
          } yield assert(removed)(equalTo(2L))
        },
        testM("when just part of elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- zRem(key, "b", "d")
          } yield assert(removed)(equalTo(1L))
        },
        testM("when none of the elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- zRem(key, "d", "e")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from an empty set") {
          for {
            key     <- uuid
            removed <- zRem(key, "a", "b")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value)
            removed <- zRem(key, "a", "b").either
          } yield assert(removed)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zRemRangeByLex")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "Mumbai"),
                   MemberScore(0d, "Hyderabad"),
                   MemberScore(0d, "Kolkata"),
                   MemberScore(0d, "Chennai")
                 )
            remResult <-
              zRemRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
            rangeResult <- zRangeByLex(key, LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded))
                             .returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Chennai", "Delhi", "Hyderabad"))) &&
            assert(remResult)(equalTo(2L))
        },
        testM("empty set") {
          for {
            key <- uuid
            remResult <-
              zRemRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByRank")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "Hyderabad"),
                   MemberScore(4d, "Kolkata"),
                   MemberScore(5d, "Chennai")
                 )
            remResult   <- zRemRangeByRank(key, 1 to 2)
            rangeResult <- zRange(key, 0 to -1).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Delhi", "Kolkata", "Chennai"))) &&
            assert(remResult)(equalTo(2L))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRemRangeByRank(key, 1 to 2)
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByScore")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(80d, "Delhi"),
                   MemberScore(60d, "Mumbai"),
                   MemberScore(70d, "Hyderabad"),
                   MemberScore(50d, "Kolkata"),
                   MemberScore(65d, "Chennai")
                 )
            remResult <- zRemRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70)))
            rangeResult <- zRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Infinity))
                             .returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Hyderabad", "Delhi"))) && assert(remResult)(equalTo(3L))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRemRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70)))
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRevRange")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(80d, "Delhi"),
                   MemberScore(60d, "Mumbai"),
                   MemberScore(70d, "Hyderabad"),
                   MemberScore(50d, "Kolkata"),
                   MemberScore(65d, "Chennai")
                 )
            revResult <- zRevRange(key, 0 to 1).returning[String]
          } yield assert(revResult.toList)(equalTo(List("Delhi", "Hyderabad")))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRevRange(key, 0 to -1).returning[String]
          } yield assert(remResult.toList)(isEmpty)
        }
      ),
      suite("zRevRangeWithScores")(
        testM("non-empty set") {
          for {
            key       <- uuid
            delhi      = MemberScore(80d, "Delhi")
            mumbai     = MemberScore(60d, "Mumbai")
            hyderabad  = MemberScore(70d, "Hyderabad")
            kolkata    = MemberScore(50d, "Kolkata")
            chennai    = MemberScore(65d, "Chennai")
            _         <- zAdd(key)(delhi, mumbai, hyderabad, kolkata, chennai)
            revResult <- zRevRangeWithScores(key, 0 to 1).returning[String]
          } yield assert(revResult.toList)(equalTo(List(delhi, hyderabad)))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRevRange(key, 0 to -1).returning[String]
          } yield assert(remResult.toList)(isEmpty)
        }
      ),
      suite("zRevRangeByLex")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "London"),
                   MemberScore(0d, "Paris"),
                   MemberScore(0d, "Tokyo"),
                   MemberScore(0d, "NewYork"),
                   MemberScore(0d, "Seoul")
                 )
            lexRange     = LexRange(min = LexMinimum.Closed("Delhi"), max = LexMaximum.Open("Seoul"))
            rangeResult <- zRevRangeByLex(key, lexRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Paris", "NewYork", "London", "Delhi")))
        },
        testM("non-empty set with limit") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "London"),
                   MemberScore(0d, "Paris"),
                   MemberScore(0d, "Tokyo"),
                   MemberScore(0d, "NewYork"),
                   MemberScore(0d, "Seoul")
                 )
            lexRange     = LexRange(min = LexMinimum.Closed("Delhi"), max = LexMaximum.Open("Seoul"))
            rangeResult <- zRevRangeByLex(key, lexRange, Some(Limit(offset = 1, count = 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("NewYork", "London")))
        },
        testM("empty set") {
          for {
            key         <- uuid
            lexRange     = LexRange(min = LexMinimum.Closed("Mumbai"), max = LexMaximum.Open("Hyderabad"))
            rangeResult <- zRevRangeByLex(key, lexRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRangeByScore")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- zRevRangeByScore(key, scoreRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Sunsui", "Nokia")))
        },
        testM("non-empty set with limit") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- zRevRangeByScore(key, scoreRange, Some(Limit(1, 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Nokia")))
        },
        testM("empty set") {
          for {
            key         <- uuid
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- zRevRangeByScore(key, scoreRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRangeByScoreWithScores")(
        testM("non-empty set") {
          for {
            key         <- uuid
            samsung      = MemberScore(1556d, "Samsung")
            nokia        = MemberScore(2000d, "Nokia")
            micromax     = MemberScore(1800d, "Micromax")
            sunsui       = MemberScore(2200d, "Sunsui")
            nicroSoft    = MemberScore(1800d, "MicroSoft")
            lg           = MemberScore(2500d, "LG")
            _           <- zAdd(key)(samsung, nokia, micromax, sunsui, nicroSoft, lg)
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- zRevRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List(sunsui, nokia)))
        },
        testM("non-empty set with limit") {
          for {
            key         <- uuid
            samsung      = MemberScore(1556d, "Samsung")
            nokia        = MemberScore(2000d, "Nokia")
            micromax     = MemberScore(1800d, "Micromax")
            sunsui       = MemberScore(2200d, "Sunsui")
            nicroSoft    = MemberScore(1800d, "MicroSoft")
            lg           = MemberScore(2500d, "LG")
            _           <- zAdd(key)(samsung, nokia, micromax, sunsui, nicroSoft, lg)
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- zRevRangeByScoreWithScores(key, scoreRange, Some(Limit(1, 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List(nokia)))
        },
        testM("empty set") {
          for {
            key         <- uuid
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- zRevRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRank")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- zRevRank(key, "Hyderabad")
          } yield assert(result)(equalTo(Some(2L)))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRevRank(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zScan")(
        testM("non-empty set") {
          for {
            key     <- uuid
            a        = MemberScore(1d, "atest")
            b        = MemberScore(2d, "btest")
            c        = MemberScore(3d, "ctest")
            _       <- zAdd(key)(a, b, c)
            members <- scanAll(key)
          } yield assert(members)(equalTo(Chunk(a, b, c)))
        },
        testM("empty set") {
          for {
            key              <- uuid
            scan             <- zScan(key, 0L).returning[String]
            (cursor, members) = scan
          } yield assert(cursor)(isZero) && assert(members)(isEmpty)
        },
        testM("with match over non-empty set") {
          for {
            key     <- uuid
            one      = MemberScore(1d, "one")
            two      = MemberScore(2d, "two")
            three    = MemberScore(3d, "three")
            _       <- zAdd(key)(one, two, three)
            members <- scanAll(key, Some("t[a-z]*"))
          } yield assert(members)(equalTo(Chunk(two, three)))
        },
        testM("with count over non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            members <- scanAll(key, count = Some(Count(3L)))
          } yield assert(members)(isNonEmpty)
        },
        testM("match with count over non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(1d, "testa"),
                   MemberScore(2d, "testb"),
                   MemberScore(3d, "testc"),
                   MemberScore(4d, "testd"),
                   MemberScore(5d, "teste")
                 )
            members <- scanAll(key, pattern = Some("t[a-z]*"), count = Some(Count(3L)))
          } yield assert(members)(isNonEmpty)
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            scan  <- zScan(key, 0L).returning[String].either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zScore")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- zScore(key, "Delhi")
          } yield assert(result)(equalTo(Some(10.0)))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zScore(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zMScore")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- zMScore(key, "Delhi", "Mumbai", "notFound")
          } yield assert(result)(equalTo(Chunk(Some(10d), Some(20d), None)))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zMScore(key, "Hyderabad")
          } yield assert(result)(equalTo(Chunk(None)))
        }
      ),
      suite("zUnion")(
        testM("two non-empty sets") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- zUnion(2, first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "b", "d", "e", "c")))
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            members  <- zUnion(2, nonEmpty, empty)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "b")))
        },
        testM("empty when both sets are empty") {
          for {
            first   <- uuid
            second  <- uuid
            members <- zUnion(2, first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first   <- uuid
            second  <- uuid
            third   <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- zAdd(second)(MemberScore(2, "b"), MemberScore(4d, "d"))
            _       <- zAdd(third)(MemberScore(2, "b"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- zUnion(3, first, second, third)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "e", "b", "c", "d")))
        },
        testM("error when the first parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- set(first, value)
            members <- zUnion(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- zAdd(first)(MemberScore(1, "a"))
            _       <- set(second, value)
            members <- zUnion(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnion(2, first, second)(Some(::(2, List(3)))).returning[String]
          } yield assert(members)(equalTo(Chunk("M", "P", "O", "N")))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnion(2, first, second)(Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnion(2, first, second)(Some(::(2, List(3, 5)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnion(2, first, second)(aggregate = Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("P", "M", "N", "O")))
        },
        testM("set aggregate parameter MIN") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnion(2, first, second)(aggregate = Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N", "P", "M")))
        },
        testM("parameter weights provided along with aggregate") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnion(2, first, second)(Some(::(2, List(3))), Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("M", "N", "P", "O")))
        }
      ),
      suite("zUnionWithScores")(
        testM("two non-empty sets") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- zUnionWithScores(2, first, second)().returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(2d, "a"),
                MemberScore(2d, "b"),
                MemberScore(4d, "d"),
                MemberScore(5d, "e"),
                MemberScore(6d, "c")
              )
            )
          )
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            members  <- zUnionWithScores(2, nonEmpty, empty)().returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(1d, "a"), MemberScore(2d, "b"))))
        },
        testM("empty when both sets are empty") {
          for {
            first   <- uuid
            second  <- uuid
            members <- zUnionWithScores(2, first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first   <- uuid
            second  <- uuid
            third   <- uuid
            _       <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- zAdd(second)(MemberScore(2, "b"), MemberScore(4d, "d"))
            _       <- zAdd(third)(MemberScore(2, "b"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- zUnionWithScores(3, first, second, third)().returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(1d, "a"),
                MemberScore(5d, "e"),
                MemberScore(6d, "b"),
                MemberScore(6d, "c"),
                MemberScore(8d, "d")
              )
            )
          )
        },
        testM("error when the first parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- set(first, value)
            members <- zUnionWithScores(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- zAdd(first)(MemberScore(1, "a"))
            _       <- set(second, value)
            members <- zUnionWithScores(2, first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnionWithScores(2, first, second)(Some(::(2, List(3)))).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(10d, "M"),
                MemberScore(12d, "P"),
                MemberScore(20d, "O"),
                MemberScore(21d, "N")
              )
            )
          )
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnionWithScores(2, first, second)(Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnionWithScores(2, first, second)(Some(::(2, List(3, 5)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnionWithScores(2, first, second)(aggregate = Some(Aggregate.Max)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(4d, "P"),
                MemberScore(5d, "M"),
                MemberScore(6d, "N"),
                MemberScore(7d, "O")
              )
            )
          )
        },
        testM("set aggregate parameter MIN") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnionWithScores(2, first, second)(aggregate = Some(Aggregate.Min)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(2d, "O"),
                MemberScore(3d, "N"),
                MemberScore(4d, "P"),
                MemberScore(5d, "M")
              )
            )
          )
        },
        testM("parameter weights provided along with aggregate") {
          for {
            first   <- uuid
            second  <- uuid
            _       <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- zUnionWithScores(2, first, second)(Some(::(2, List(3))), Some(Aggregate.Max)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(10d, "M"),
                MemberScore(12d, "N"),
                MemberScore(12d, "P"),
                MemberScore(14d, "O")
              )
            )
          )
        }
      ),
      suite("zUnionStore")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _      <- zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card   <- zUnionStore(dest, 2, first, second)()
          } yield assert(card)(equalTo(5L))
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            dest     <- uuid
            _        <- zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            card     <- zUnionStore(dest, 2, nonEmpty, empty)()
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            card   <- zUnionStore(dest, 2, first, second)()
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _      <- zAdd(second)(MemberScore(2, "b"), MemberScore(4d, "d"))
            _      <- zAdd(third)(MemberScore(2, "b"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card   <- zUnionStore(dest, 3, first, second, third)()
          } yield assert(card)(equalTo(5L))
        },
        testM("error when the first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- set(first, value)
            card   <- zUnionStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- zAdd(first)(MemberScore(1, "a"))
            _      <- set(second, value)
            card   <- zUnionStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(Some(::(2, List(3))))
          } yield assert(card)(equalTo(4L))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(Some(::(2, Nil))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(Some(::(2, List(3, 5)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(aggregate = Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        },
        testM("set aggregate parameter MIN") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(aggregate = Some(Aggregate.Min))
          } yield assert(card)(equalTo(4L))
        },
        testM("parameter weights provided along with aggregate") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(Some(::(2, List(3))), Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        }
      ),
      suite("zRandMember")(
        testM("key does not exist") {
          for {
            first     <- uuid
            notExists <- uuid
            _         <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret       <- zRandMember(notExists).returning[String]
          } yield assert(ret)(isNone)
        },
        testM("key does not exist with count") {
          for {
            first     <- uuid
            notExists <- uuid
            _         <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret       <- zRandMember(notExists, 1).returning[String]
          } yield assert(ret)(isEmpty)
        },
        testM("get an element") {
          for {
            first <- uuid
            _     <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret   <- zRandMember(first).returning[String]
          } yield assert(ret)(isSome)
        },
        testM("get elements with count") {
          for {
            first <- uuid
            _     <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret   <- zRandMember(first, 2).returning[String]
          } yield assert(ret)(hasSize(equalTo(2)))
        }
      ),
      suite("zRandMemberWithScores")(
        testM("key does not exist") {
          for {
            first     <- uuid
            notExists <- uuid
            _         <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret       <- zRandMemberWithScores(notExists, 1).returning[String]
          } yield assert(ret)(isEmpty)
        },
        testM("get elements with count") {
          for {
            first <- uuid
            _     <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret   <- zRandMemberWithScores(first, 2).returning[String]
          } yield assert(ret)(hasSize(equalTo(2)))
        }
      )
    )

  private def scanAll(
    key: String,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[MemberScore[String]]] =
    ZStream
      .paginateChunkM(0L) { cursor =>
        zScan(key, cursor, pattern, count).returning[String].map {
          case (nc, nm) if nc == 0 => (nm, None)
          case (nc, nm)            => (nm, Some(nc))
        }
      }
      .runCollect

}
