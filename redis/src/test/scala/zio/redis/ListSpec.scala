package zio.redis

import java.util.concurrent.TimeUnit

import zio.clock.{ Clock, currentTime }
import zio.duration._
import zio.redis.RedisError.WrongType
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Has }

trait ListSpec extends BaseSpec {
  val listSuite
    : Spec[Has[Clock.Service] with Has[RedisExecutor.Service], TestFailure[java.io.Serializable], TestSuccess] =
    suite("lists")(
      suite("pop")(
        testM("lPop non-empty list") {
          for {
            key    <- uuid
            _      <- lPush(key, "world", "hello")
            popped <- lPop(key).returning[String]
          } yield assert(popped)(isSome(equalTo("hello")))
        },
        testM("lPop empty list") {
          for {
            popped <- lPop("unknown").returning[String]
          } yield assert(popped)(isNone)
        },
        testM("lPop error not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            pop   <- lPop(key).returning[String].either
          } yield assert(pop)(isLeft)
        },
        testM("rPop non-empty list") {
          for {
            key <- uuid
            _   <- rPush(key, "world", "hello")
            pop <- rPop(key).returning[String]
          } yield assert(pop)(isSome(equalTo("hello")))
        },
        testM("rPop empty list") {
          for {
            pop <- rPop("unknown").returning[String]
          } yield assert(pop)(isNone)
        },
        testM("rPop error not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            pop   <- rPop(key).returning[String].either
          } yield assert(pop)(isLeft)
        }
      ),
      suite("push")(
        testM("lPush onto empty list") {
          for {
            key   <- uuid
            push  <- lPush(key, "hello")
            range <- lRange(key, 0 to -1).returning[String]
          } yield assert(push)(equalTo(1L)) && assert(range)(equalTo(Chunk("hello")))
        },
        testM("lPush multiple elements onto empty list") {
          for {
            key   <- uuid
            push  <- lPush(key, "hello", "world")
            range <- lRange(key, 0 to -1).returning[String]
          } yield assert(push)(equalTo(2L)) && assert(range)(equalTo(Chunk("world", "hello")))
        },
        testM("lPush error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            push  <- lPush(key, "hello").either
          } yield assert(push)(isLeft)
        },
        testM("lPushX onto non-empty list") {
          for {
            key   <- uuid
            _     <- lPush(key, "world")
            px    <- lPushX(key, "hello")
            range <- lRange(key, 0 to -1).returning[String]
          } yield assert(px)(equalTo(2L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lPushX nothing when key doesn't exist") {
          for {
            key <- uuid
            px  <- lPushX(key, "world")
          } yield assert(px)(equalTo(0L))
        },
        testM("lPushX error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            push  <- lPushX(key, "hello").either
          } yield assert(push)(isLeft(isSubtype[RedisError.WrongType](anything)))
        },
        testM("rPush onto empty list") {
          for {
            key   <- uuid
            push  <- rPush(key, "hello")
            range <- lRange(key, 0 to -1).returning[String]
          } yield assert(push)(equalTo(1L)) && assert(range)(equalTo(Chunk("hello")))
        },
        testM("rPush multiple elements onto empty list") {
          for {
            key   <- uuid
            push  <- rPush(key, "hello", "world")
            range <- lRange(key, 0 to -1).returning[String]
          } yield assert(push)(equalTo(2L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("rPush error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            push  <- rPush(key, "hello").either
          } yield assert(push)(isLeft)
        },
        testM("rPushX onto non-empty list") {
          for {
            key   <- uuid
            _     <- rPush(key, "world")
            px    <- rPushX(key, "hello")
            range <- lRange(key, 0 to -1).returning[String]
          } yield assert(px)(equalTo(2L)) && assert(range)(equalTo(Chunk("world", "hello")))
        },
        testM("rPushX nothing when key doesn't exist") {
          for {
            key <- uuid
            px  <- rPushX(key, "world")
          } yield assert(px)(equalTo(0L))
        },
        testM("rPushX error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            push  <- rPushX(key, "hello").either
          } yield assert(push)(isLeft(isSubtype[RedisError.WrongType](anything)))
        }
      ),
      suite("poppush")(
        testM("rPopLPush") {
          for {
            key  <- uuid
            dest <- uuid
            _    <- rPush(key, "one", "two", "three")
            _    <- rPush(dest, "four")
            _    <- rPopLPush(key, dest).returning[String]
            r    <- lRange(key, 0 to -1).returning[String]
            l    <- lRange(dest, 0 to -1).returning[String]
          } yield assert(r)(equalTo(Chunk("one", "two"))) && assert(l)(equalTo(Chunk("three", "four")))
        },
        testM("rPopLPush nothing when source does not exist") {
          for {
            key  <- uuid
            dest <- uuid
            _    <- rPush(dest, "four")
            _    <- rPopLPush(key, dest).returning[String]
            l    <- lRange(dest, 0 to -1).returning[String]
          } yield assert(l)(equalTo(Chunk("four")))
        },
        testM("rPopLPush error when not list") {
          for {
            key   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- set(key, value)
            rpp   <- rPopLPush(key, dest).returning[String].either
          } yield assert(rpp)(isLeft)
        }
      ),
      suite("blocking poppush")(
        testM("brPopLPush") {
          for {
            key  <- uuid
            dest <- uuid
            _    <- rPush(key, "one", "two", "three")
            _    <- rPush(dest, "four")
            _    <- brPopLPush(key, dest, 1.seconds).returning[String]
            r    <- lRange(key, 0 to -1).returning[String]
            l    <- lRange(dest, 0 to -1).returning[String]
          } yield assert(r)(equalTo(Chunk("one", "two"))) && assert(l)(equalTo(Chunk("three", "four")))
        },
        testM("brPopLPush block for 1 second when source does not exist") {
          for {
            key     <- uuid
            dest    <- uuid
            _       <- rPush(dest, "four")
            st      <- currentTime(TimeUnit.SECONDS)
            s       <- brPopLPush(key, dest, 1.seconds).returning[String]
            endTime <- currentTime(TimeUnit.SECONDS)
          } yield assert(s)(isNone) && assert(endTime - st)(isGreaterThanEqualTo(1L))
        },
        testM("brPopLPush error when not list") {
          for {
            key   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- set(key, value)
            bpp   <- brPopLPush(key, dest, 1.seconds).returning[String].either
          } yield assert(bpp)(isLeft)
        }
      ),
      suite("remove")(
        testM("lRem 2 elements moving from head") {
          for {
            key     <- uuid
            _       <- lPush(key, "world", "hello", "hello", "hello")
            removed <- lRem(key, 2, "hello")
            range   <- lRange(key, 0 to 1).returning[String]
          } yield assert(removed)(equalTo(2L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRem 2 elements moving from tail") {
          for {
            key     <- uuid
            _       <- lPush(key, "hello", "hello", "world", "hello")
            removed <- lRem(key, -2, "hello")
            range   <- lRange(key, 0 to 1).returning[String]
          } yield assert(removed)(equalTo(2L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRem all 3 'hello' elements") {
          for {
            key     <- uuid
            _       <- lPush(key, "hello", "hello", "world", "hello")
            removed <- lRem(key, 0, "hello")
            range   <- lRange(key, 0 to 1).returning[String]
          } yield assert(removed)(equalTo(3L)) && assert(range)(equalTo(Chunk("world")))
        },
        testM("lRem nothing when key does not exist") {
          for {
            key     <- uuid
            _       <- lPush(key, "world", "hello")
            removed <- lRem(key, 0, "goodbye")
            range   <- lRange(key, 0 to 1).returning[String]
          } yield assert(removed)(equalTo(0L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRem error when not list") {
          for {
            key     <- uuid
            _       <- set(key, "hello")
            removed <- lRem(key, 0, "hello").either
          } yield assert(removed)(isLeft)
        }
      ),
      suite("set")(
        testM("lSet element") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            _     <- lSet(key, 1, "goodbye")
            range <- lRange(key, 0 to 1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello", "goodbye")))
        },
        testM("lSet error when index out of bounds") {
          for {
            key <- uuid
            _   <- lPush(key, "world", "hello")
            set <- lSet(key, 2, "goodbye").either
          } yield assert(set)(isLeft)
        },
        testM("lSet error when not list") {
          for {
            key <- uuid
            _   <- set(key, "hello")
            set <- lSet(key, 0, "goodbye").either
          } yield assert(set)(isLeft)
        }
      ),
      suite("length")(
        testM("lLen non-empty list") {
          for {
            key <- uuid
            _   <- lPush(key, "world", "hello")
            len <- lLen(key)
          } yield assert(len)(equalTo(2L))
        },
        testM("lLen 0 when no key") {
          for {
            len <- lLen("unknown")
          } yield assert(len)(equalTo(0L))
        },
        testM("lLen error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            index <- lLen(key).either
          } yield assert(index)(isLeft)
        }
      ),
      suite("range")(
        testM("lRange two elements") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            range <- lRange(key, 0 to 1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRange two elements negative indices") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            range <- lRange(key, -2 to -1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRange start out of bounds") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            range <- lRange(key, 2 to 3).returning[String]
          } yield assert(range)(equalTo(Chunk()))
        },
        testM("lRange end out of bounds") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            range <- lRange(key, 1 to 2).returning[String]
          } yield assert(range)(equalTo(Chunk("world")))
        },
        testM("lRange error when not list") {
          for {
            key   <- uuid
            _     <- set(key, "hello")
            range <- lRange(key, 1 to 2).returning[String].either
          } yield assert(range)(isLeft)
        }
      ),
      suite("index element")(
        testM("lIndex first element") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            index <- lIndex(key, 0L).returning[String]
          } yield assert(index)(isSome(equalTo("hello")))
        },
        testM("lIndex last element") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            index <- lIndex(key, -1L).returning[String]
          } yield assert(index)(isSome(equalTo("world")))
        },
        testM("lIndex no existing element") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            index <- lIndex(key, 3).returning[String]
          } yield assert(index)(isNone)
        },
        testM("lIndex error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            index <- lIndex(key, -1L).returning[String].either
          } yield assert(index)(isLeft)
        }
      ),
      suite("trim element")(
        testM("lTrim element") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            _     <- lTrim(key, 0 to 0)
            range <- lRange(key, 0 to -1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello")))
        },
        testM("lTrim start index out of bounds") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            _     <- lTrim(key, 2 to 5)
            range <- lRange(key, 0 to 1).returning[String]
          } yield assert(range)(equalTo(Chunk()))
        },
        testM("lTrim end index out of bounds") {
          for {
            key   <- uuid
            _     <- lPush(key, "world", "hello")
            _     <- lTrim(key, 0 to 3)
            range <- lRange(key, 0 to 1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lTrim error when not list") {
          for {
            key  <- uuid
            _    <- set(key, "hello")
            trim <- lTrim(key, 0 to 3).either
          } yield assert(trim)(isLeft)
        }
      ),
      suite("blPop")(
        testM("from single list") {
          for {
            key        <- uuid
            _          <- lPush(key, "a", "b", "c")
            popped     <- blPop[String, String](key)(1.second).some
            (src, elem) = popped
          } yield assert(src)(equalTo(key)) && assert(elem)(equalTo("c"))
        },
        testM("from one empty and one non-empty list") {
          for {
            empty      <- uuid
            nonEmpty   <- uuid
            _          <- lPush(nonEmpty, "a", "b", "c")
            popped     <- blPop[String, String](empty, nonEmpty)(1.second).some
            (src, elem) = popped
          } yield assert(src)(equalTo(nonEmpty)) && assert(elem)(equalTo("c"))
        },
        testM("from one empty list") {
          for {
            key    <- uuid
            popped <- blPop[String, String](key)(1.second)
          } yield assert(popped)(isNone)
        },
        testM("from multiple empty lists") {
          for {
            first  <- uuid
            second <- uuid
            popped <- blPop[String, String](first, second)(1.second)
          } yield assert(popped)(isNone)
        },
        testM("from non-empty list with timeout 0s") {
          for {
            key        <- uuid
            _          <- lPush(key, "a", "b", "c")
            popped     <- blPop[String, String](key)(0.seconds).some
            (src, elem) = popped
          } yield assert(src)(equalTo(key)) && assert(elem)(equalTo("c"))
        },
        testM("from not list") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            popped <- blPop[String, String](key)(1.second).either
          } yield assert(popped)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("brPop")(
        testM("from single list") {
          for {
            key        <- uuid
            _          <- lPush(key, "a", "b", "c")
            popped     <- brPop[String, String](key)(1.second).some
            (src, elem) = popped
          } yield assert(src)(equalTo(key)) && assert(elem)(equalTo("a"))
        },
        testM("from one empty and one non-empty list") {
          for {
            empty      <- uuid
            nonEmpty   <- uuid
            _          <- lPush(nonEmpty, "a", "b", "c")
            popped     <- brPop[String, String](empty, nonEmpty)(1.second).some
            (src, elem) = popped
          } yield assert(src)(equalTo(nonEmpty)) && assert(elem)(equalTo("a"))
        },
        testM("from one empty list") {
          for {
            key    <- uuid
            popped <- brPop[String, String](key)(1.second)
          } yield assert(popped)(isNone)
        },
        testM("from multiple empty lists") {
          for {
            first  <- uuid
            second <- uuid
            popped <- brPop[String, String](first, second)(1.second)
          } yield assert(popped)(isNone)
        },
        testM("from non-empty list with timeout 0s") {
          for {
            key        <- uuid
            _          <- lPush(key, "a", "b", "c")
            popped     <- brPop[String, String](key)(0.seconds).some
            (src, elem) = popped
          } yield assert(src)(equalTo(key)) && assert(elem)(equalTo("a"))
        },
        testM("from not list") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            popped <- brPop[String, String](key)(1.second).either
          } yield assert(popped)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("lInsert")(
        testM("before pivot into non-empty list") {
          for {
            key <- uuid
            _   <- lPush(key, "a", "b", "c")
            len <- lInsert(key, Position.Before, "b", "d")
          } yield assert(len)(equalTo(4L))
        },
        testM("after pivot into non-empty list") {
          for {
            key <- uuid
            _   <- lPush(key, "a", "b", "c")
            len <- lInsert(key, Position.After, "b", "d")
          } yield assert(len)(equalTo(4L))
        },
        testM("before pivot into empty list") {
          for {
            key <- uuid
            len <- lInsert(key, Position.Before, "a", "b")
          } yield assert(len)(equalTo(0L))
        },
        testM("after pivot into empty list") {
          for {
            key <- uuid
            len <- lInsert(key, Position.After, "a", "b")
          } yield assert(len)(equalTo(0L))
        },
        testM("before pivot that doesn't exist") {
          for {
            key <- uuid
            _   <- lPush(key, "a", "b", "c")
            len <- lInsert(key, Position.Before, "unknown", "d")
          } yield assert(len)(equalTo(-1L))
        },
        testM("after pivot that doesn't exist") {
          for {
            key <- uuid
            _   <- lPush(key, "a", "b", "c")
            len <- lInsert(key, Position.After, "unknown", "d")
          } yield assert(len)(equalTo(-1L))
        },
        testM("error before pivot into not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            len   <- lInsert(key, Position.Before, "a", "b").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error after pivot into not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            len   <- lInsert(key, Position.After, "a", "b").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("lMove")(
        testM("move from source to destination left right") {
          for {
            source           <- uuid
            destination      <- uuid
            _                <- rPush(source, "a", "b", "c")
            _                <- rPush(destination, "d")
            moved            <- lMove(source, destination, Side.Left, Side.Right).returning[String]
            sourceRange      <- lRange(source, 0 to -1).returning[String]
            destinationRange <- lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("b", "c"))) && assert(destinationRange)(equalTo(Chunk("d", "a")))
        },
        testM("move from source to destination right left") {
          for {
            source           <- uuid
            destination      <- uuid
            _                <- rPush(source, "a", "b", "c")
            _                <- rPush(destination, "d")
            moved            <- lMove(source, destination, Side.Right, Side.Left).returning[String]
            sourceRange      <- lRange(source, 0 to -1).returning[String]
            destinationRange <- lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b"))) &&
            assert(destinationRange)(equalTo(Chunk("c", "d")))
        },
        testM("move from source to destination left left") {
          for {
            source           <- uuid
            destination      <- uuid
            _                <- rPush(source, "a", "b", "c")
            _                <- rPush(destination, "d")
            moved            <- lMove(source, destination, Side.Left, Side.Left).returning[String]
            sourceRange      <- lRange(source, 0 to -1).returning[String]
            destinationRange <- lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("b", "c"))) &&
            assert(destinationRange)(equalTo(Chunk("a", "d")))
        },
        testM("move from source to destination right right") {
          for {
            source           <- uuid
            destination      <- uuid
            _                <- rPush(source, "a", "b", "c")
            _                <- rPush(destination, "d")
            moved            <- lMove(source, destination, Side.Right, Side.Right).returning[String]
            sourceRange      <- lRange(source, 0 to -1).returning[String]
            destinationRange <- lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b"))) &&
            assert(destinationRange)(equalTo(Chunk("d", "c")))
        },
        testM("move from source to source left right") {
          for {
            source      <- uuid
            _           <- rPush(source, "a", "b", "c")
            moved       <- lMove(source, source, Side.Left, Side.Right).returning[String]
            sourceRange <- lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) && assert(sourceRange)(equalTo(Chunk("b", "c", "a")))
        },
        testM("move from source to source right left") {
          for {
            source      <- uuid
            _           <- rPush(source, "a", "b", "c")
            moved       <- lMove(source, source, Side.Right, Side.Left).returning[String]
            sourceRange <- lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("c", "a", "b")))
        },
        testM("move from source to source left left") {
          for {
            source      <- uuid
            _           <- rPush(source, "a", "b", "c")
            moved       <- lMove(source, source, Side.Left, Side.Left).returning[String]
            sourceRange <- lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b", "c")))
        },
        testM("move from source to source right right") {
          for {
            source      <- uuid
            _           <- rPush(source, "a", "b", "c")
            moved       <- lMove(source, source, Side.Right, Side.Right).returning[String]
            sourceRange <- lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) && assert(sourceRange)(equalTo(Chunk("a", "b", "c")))
        },
        testM("return nil when source dose not exist") {
          for {
            source      <- uuid
            destination <- uuid
            _           <- rPush(destination, "d")
            moved       <- lMove(source, destination, Side.Left, Side.Right).returning[String]
          } yield assert(moved)(isNone)
        }
      ),
      suite("blMove")(
        testM("move from source to destination left right") {
          for {
            source           <- uuid
            destination      <- uuid
            _                <- rPush(source, "a", "b", "c")
            _                <- rPush(destination, "d")
            moved            <- blMove(source, destination, Side.Left, Side.Right, 1.second).returning[String]
            sourceRange      <- lRange(source, 0 to -1).returning[String]
            destinationRange <- lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("b", "c"))) &&
            assert(destinationRange)(equalTo(Chunk("d", "a")))
        },
        testM("move from source to destination right left") {
          for {
            source           <- uuid
            destination      <- uuid
            _                <- rPush(source, "a", "b", "c")
            _                <- rPush(destination, "d")
            moved            <- blMove(source, destination, Side.Right, Side.Left, 1.second).returning[String]
            sourceRange      <- lRange(source, 0 to -1).returning[String]
            destinationRange <- lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b"))) &&
            assert(destinationRange)(equalTo(Chunk("c", "d")))
        },
        testM("move from source to destination left left") {
          for {
            source           <- uuid
            destination      <- uuid
            _                <- rPush(source, "a", "b", "c")
            _                <- rPush(destination, "d")
            moved            <- blMove(source, destination, Side.Left, Side.Left, 1.second).returning[String]
            sourceRange      <- lRange(source, 0 to -1).returning[String]
            destinationRange <- lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("b", "c"))) &&
            assert(destinationRange)(equalTo(Chunk("a", "d")))
        },
        testM("move from source to destination right right") {
          for {
            source           <- uuid
            destination      <- uuid
            _                <- rPush(source, "a", "b", "c")
            _                <- rPush(destination, "d")
            moved            <- blMove(source, destination, Side.Right, Side.Right, 1.second).returning[String]
            sourceRange      <- lRange(source, 0 to -1).returning[String]
            destinationRange <- lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b"))) &&
            assert(destinationRange)(equalTo(Chunk("d", "c")))
        },
        testM("move from source to source left right") {
          for {
            source      <- uuid
            _           <- rPush(source, "a", "b", "c")
            moved       <- blMove(source, source, Side.Left, Side.Right, 1.second).returning[String]
            sourceRange <- lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) && assert(sourceRange)(equalTo(Chunk("b", "c", "a")))
        },
        testM("move from source to source right left") {
          for {
            source      <- uuid
            _           <- rPush(source, "a", "b", "c")
            moved       <- blMove(source, source, Side.Right, Side.Left, 1.second).returning[String]
            sourceRange <- lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) && assert(sourceRange)(equalTo(Chunk("c", "a", "b")))
        },
        testM("move from source to source left left") {
          for {
            source      <- uuid
            _           <- rPush(source, "a", "b", "c")
            moved       <- blMove(source, source, Side.Left, Side.Left, 1.second).returning[String]
            sourceRange <- lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) && assert(sourceRange)(equalTo(Chunk("a", "b", "c")))
        },
        testM("move from source to source right right") {
          for {
            source      <- uuid
            _           <- rPush(source, "a", "b", "c")
            moved       <- blMove(source, source, Side.Right, Side.Right, 1.second).returning[String]
            sourceRange <- lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) && assert(sourceRange)(equalTo(Chunk("a", "b", "c")))
        },
        testM("block until timeout reached and return nil") {
          for {
            source      <- uuid
            destination <- uuid
            _           <- rPush(destination, "d")
            startTime   <- currentTime(TimeUnit.SECONDS)
            moved       <- blMove(source, destination, Side.Left, Side.Right, 1.second).returning[String]
            endTime     <- currentTime(TimeUnit.SECONDS)
          } yield assert(moved)(isNone) && assert(endTime - startTime)(isGreaterThanEqualTo(1L))
        }
      ),
      suite("lPos")(
        testM("find index of element") {
          for {
            key <- uuid
            _   <- rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx <- lPos(key, "3")
          } yield assert(idx)(isSome(equalTo(6L)))
        },
        testM("don't find index of element") {
          for {
            key <- uuid
            _   <- rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx <- lPos(key, "unknown")
          } yield assert(idx)(isNone)
        },
        testM("find index of element with positive rank") {
          for {
            key <- uuid
            _   <- rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx <- lPos(key, "3", rank = Some(Rank(2)))
          } yield assert(idx)(isSome(equalTo(8L)))
        },
        testM("find index of element with negative rank") {
          for {
            key <- uuid
            _   <- rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx <- lPos(key, "3", rank = Some(Rank(-1)))
          } yield assert(idx)(isSome(equalTo(10L)))
        },
        testM("find index of element with maxLen") {
          for {
            key <- uuid
            _   <- rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx <- lPos(key, "3", maxLen = Some(ListMaxLen(8)))
          } yield assert(idx)(isSome(equalTo(6L)))
        },
        testM("don't find index of element with maxLen") {
          for {
            key <- uuid
            _   <- rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx <- lPos(key, "3", maxLen = Some(ListMaxLen(5)))
          } yield assert(idx)(isNone)
        }
      ),
      suite("lPosCount")(
        testM("find index of element with rank and count") {
          for {
            key <- uuid
            _   <- rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx <- lPosCount(key, "3", Count(2), rank = Some(Rank(2)))
          } yield assert(idx)(equalTo(Chunk(8L, 9L)))
        },
        testM("find index of element with negative rank and count") {
          for {
            key <- uuid
            _   <- rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx <- lPosCount(key, "3", Count(2), rank = Some(Rank(-3)))
          } yield assert(idx)(equalTo(Chunk(8L, 6L)))
        }
      )
    )
}
