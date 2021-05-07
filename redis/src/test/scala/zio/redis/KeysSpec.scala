package zio.redis

import zio.clock.Clock
import zio.duration._
import zio.logging.Logging
import zio.random.Random
import zio.redis.RedisError.ProtocolError
import zio.redis.codec.StringUtf8Codec
import zio.schema.codec.Codec
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ Chunk, ZIO, ZLayer }

trait KeysSpec extends BaseSpec {

  val keysSuite: Spec[Annotations with RedisExecutor with Random with TestConfig with ZTestEnv with Clock, TestFailure[
    RedisError
  ], TestSuccess] = {
    suite("keys")(
      testM("set followed by get") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value)
          v     <- get(key).returning[String]
        } yield assert(v)(isSome(equalTo(value)))
      },
      testM("get non-existing key") {
        for {
          key <- uuid
          v   <- get(key).returning[String]
        } yield assert(v)(isNone)
      },
      testM("handles wrong types") {
        for {
          key <- uuid
          _   <- sAdd(key, "1", "2", "3")
          v   <- get(key).returning[String].either
        } yield assert(v)(isLeft)
      },
      testM("check whether or not key exists") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value)
          e1    <- exists(key)
          e2    <- exists("unknown")
        } yield assert(e1)(equalTo(1L)) && assert(e2)(equalTo(0L))
      },
      testM("delete existing key") {
        for {
          key     <- uuid
          value   <- uuid
          _       <- set(key, value)
          deleted <- del(key)
        } yield assert(deleted)(equalTo(1L))
      },
      testM("find all keys matching pattern") {
        for {
          value    <- uuid
          key1      = "custom_key_1"
          key2      = "custom_key_2"
          _        <- set(key1, value)
          _        <- set(key2, value)
          response <- keys[String]("*custom*")
        } yield assert(response)(hasSameElements(Chunk(key1, key2)))
      },
      testM("unlink existing key") {
        for {
          key     <- uuid
          value   <- uuid
          _       <- set(key, value)
          removed <- unlink(key)
        } yield assert(removed)(equalTo(1L))
      },
      testM("touch two existing keys") {
        for {
          key1    <- uuid
          value1  <- uuid
          _       <- set(key1, value1)
          key2    <- uuid
          value2  <- uuid
          _       <- set(key2, value2)
          touched <- touch(key1, key2)
        } yield assert(touched)(equalTo(2L))
      },
      testM("scan entries with match, count and type options")(
        checkM(genPatternOption, genCountOption, genStringRedisTypeOption) { (pattern, count, redisType) =>
          for {
            key             <- uuid
            value           <- uuid
            _               <- set(key, value)
            scan            <- scan[String](0L, pattern, count, redisType)
            (next, elements) = scan
          } yield assert(next)(isGreaterThanEqualTo(0L)) && assert(elements)(isNonEmpty)
        }
      ) @@ flaky,
      testM("fetch random key") {
        for {
          key       <- uuid
          value     <- uuid
          _         <- set(key, value)
          allKeys   <- keys[String]("*")
          randomKey <- randomKey[String]()
        } yield assert(allKeys)(contains(randomKey.get))
      } @@ ignore,
      testM("dump followed by restore") {
        for {
          key      <- uuid
          value    <- uuid
          _        <- set(key, value)
          dumped   <- dump(key)
          _        <- del(key)
          restore  <- restore(key, 0L, dumped).either
          restored <- get(key).returning[String]
        } yield assert(restore)(isRight) && assert(restored)(isSome(equalTo(value)))
      } @@ ignore,
      suite("migrate")(
        testM("migrate key to another redis server (copy and replace)") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            response <- migrate(
                          "redis2",
                          6379,
                          key,
                          0L,
                          KeysSpec.MigrateTimeout,
                          copy = Option(Copy),
                          replace = Option(Replace),
                          keys = None
                        )
            originGet <- get(key).returning[String]
            destGet   <- get(key).returning[String].provideLayer(KeysSpec.SecondExecutor)
          } yield assert(response)(equalTo("OK")) &&
            assert(originGet)(isSome(equalTo(value))) &&
            assert(destGet)(isSome(equalTo(value)))
        },
        testM("migrate key to another redis server (move and replace)") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            response <-
              migrate(
                "redis2",
                6379,
                key,
                0L,
                KeysSpec.MigrateTimeout,
                copy = None,
                replace = Option(Replace),
                keys = None
              )
            originGet <- get(key).returning[String]
            destGet   <- get(key).returning[String].provideLayer(KeysSpec.SecondExecutor)
          } yield assert(response)(equalTo("OK")) &&
            assert(originGet)(isNone) &&
            assert(destGet)(isSome(equalTo(value)))
        },
        testM("migrate key to another redis server (move and no replace, should fail when key exists)") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            _     <- set(key, value).provideLayer(KeysSpec.SecondExecutor) // also add to second Redis
            response <-
              migrate("redis2", 6379, key, 0L, KeysSpec.MigrateTimeout, copy = None, replace = None, keys = None).either
          } yield assert(response)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("ttl")(
        testM("check ttl for existing key") {
          for {
            key   <- uuid
            value <- uuid
            _     <- pSetEx(key, 1000.millis, value)
            ttl   <- ttl(key).either
          } yield assert(ttl)(isRight)
        } @@ eventually,
        testM("check ttl for non-existing key") {
          for {
            key <- uuid
            ttl <- ttl(key).either
          } yield assert(ttl)(isLeft)
        },
        testM("check pTtl for existing key") {
          for {
            key   <- uuid
            value <- uuid
            _     <- pSetEx(key, 1000.millis, value)
            pTtl  <- pTtl(key).either
          } yield assert(pTtl)(isRight)
        } @@ eventually,
        testM("check pTtl for non-existing key") {
          for {
            key  <- uuid
            pTtl <- pTtl(key).either
          } yield assert(pTtl)(isLeft)
        }
      ),
      suite("expiration")(
        testM("set key expiration with pExpire command") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            exp       <- pExpire(key, 2000.millis)
            response1 <- exists(key)
            _         <- ZIO.sleep(2050.millis)
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        } @@ eventually,
        testM("set key expiration with pExpireAt command") {
          for {
            key       <- uuid
            value     <- uuid
            expiresAt <- instantOf(2000)
            _         <- set(key, value)
            exp       <- pExpireAt(key, expiresAt)
            response1 <- exists(key)
            _         <- ZIO.sleep(2050.millis)
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        } @@ eventually,
        testM("expire followed by persist") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            exp       <- expire(key, 10000.millis)
            persisted <- persist(key)
          } yield assert(exp)(isTrue) && assert(persisted)(isTrue)
        },
        testM("set key expiration with expireAt command") {
          for {
            key       <- uuid
            value     <- uuid
            expiresAt <- instantOf(2000)
            _         <- set(key, value)
            exp       <- expireAt(key, expiresAt)
            response1 <- exists(key)
            _         <- ZIO.sleep(2050.millis)
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        } @@ eventually
      ),
      suite("renaming")(
        testM("rename existing key") {
          for {
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- set(key, value)
            renamed <- rename(key, newKey).either
            v       <- get(newKey).returning[String]
          } yield assert(renamed)(isRight) && assert(v)(isSome(equalTo(value)))
        },
        testM("try to rename non-existing key") {
          for {
            key     <- uuid
            newKey  <- uuid
            renamed <- rename(key, newKey).either
          } yield assert(renamed)(isLeft)
        },
        testM("rename key to newkey if newkey does not yet exist") {
          for {
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- set(key, value)
            renamed <- renameNx(key, newKey)
            v       <- get(newKey).returning[String]
          } yield assert(renamed)(isTrue) && assert(v)(isSome(equalTo(value)))
        },
        testM("try to rename non-existing key with renameNx command") {
          for {
            key     <- uuid
            newKey  <- uuid
            renamed <- renameNx(key, newKey).either
          } yield assert(renamed)(isLeft)
        }
      ),
      suite("types")(
        testM("string type") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            string <- typeOf(key)
          } yield assert(string)(equalTo(RedisType.String))
        },
        testM("list type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- lPush(key, value)
            list  <- typeOf(key)
          } yield assert(list)(equalTo(RedisType.List))
        },
        testM("set type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- sAdd(key, value)
            set   <- typeOf(key)
          } yield assert(set)(equalTo(RedisType.Set))
        },
        testM("sorted set type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- zAdd(key)(MemberScore(1d, value))
            zset  <- typeOf(key)
          } yield assert(zset)(equalTo(RedisType.SortedSet))
        },
        testM("hash type") {
          for {
            key   <- uuid
            field <- uuid
            value <- uuid
            _     <- hSet(key, (field, value))
            hash  <- typeOf(key)
          } yield assert(hash)(equalTo(RedisType.Hash))
        },
        testM("stream type") {
          for {
            key    <- uuid
            field  <- uuid
            value  <- uuid
            _      <- xAdd[String, String, String, String, String](key, "*", (field, value))
            stream <- typeOf(key)
          } yield assert(stream)(equalTo(RedisType.Stream))
        }
      ),
      suite("sort")(
        testM("list of numbers") {
          for {
            key    <- uuid
            _      <- lPush(key, "1", "0", "2")
            sorted <- sort[String, String](key)
          } yield assert(sorted)(equalTo(Chunk("0", "1", "2")))
        },
        testM("list of strings") {
          for {
            key    <- uuid
            _      <- lPush(key, "z", "a", "c")
            sorted <- sort[String, String](key, alpha = Some(Alpha))
          } yield assert(sorted)(equalTo(Chunk("a", "c", "z")))
        },
        testM("list of numbers, limited") {
          for {
            key    <- uuid
            _      <- lPush(key, "1", "0", "2")
            sorted <- sort[String, String](key, limit = Some(Limit(1, 1)))
          } yield assert(sorted)(equalTo(Chunk("1")))
        },
        testM("descending sort") {
          for {
            key    <- uuid
            _      <- lPush(key, "1", "0", "2")
            sorted <- sort[String, String](key, order = Order.Descending)
          } yield assert(sorted)(equalTo(Chunk("2", "1", "0")))
        },
        testM("by the value referenced by a key-value pair") {
          for {
            key    <- uuid
            a      <- uuid
            b      <- uuid
            _      <- lPush(key, b, a)
            prefix <- uuid
            _      <- set(s"${prefix}_$a", "A")
            _      <- set(s"${prefix}_$b", "B")
            sorted <- sort[String, String](key, by = Some(s"${prefix}_*"), alpha = Some(Alpha))
          } yield assert(sorted)(equalTo(Chunk(a, b)))
        },
        testM("getting the value referenced by a key-value pair") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- lPush(key, value)
            prefix <- uuid
            _      <- set(s"${prefix}_$value", "A")
            sorted <- sort[String, String](key, get = Some((s"${prefix}_*", List.empty)), alpha = Some(Alpha))
          } yield assert(sorted)(equalTo(Chunk("A")))
        },
        testM("getting multiple value referenced by a key-value pair") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- lPush(key, value)
            prefix  <- uuid
            _       <- set(s"${prefix}_$value", "A")
            prefix2 <- uuid
            _       <- set(s"${prefix2}_$value", "0")
            sorted <-
              sort[String, String](key, get = Some((s"${prefix}_*", List(s"${prefix2}_*"))), alpha = Some(Alpha))
          } yield assert(sorted)(equalTo(Chunk("A", "0")))
        },
        testM("sort and store result") {
          for {
            key       <- uuid
            resultKey <- uuid
            _         <- lPush(key, "1", "0", "2")
            count     <- sortStore(key, Store(resultKey))
            sorted    <- lRange[String, String](resultKey, Range(0, 2))
          } yield assert(sorted)(equalTo(Chunk("0", "1", "2"))) && assert(count)(equalTo(3L))
        }
      )
    )
  }
}

object KeysSpec {
  final val MigrateTimeout: Duration = 5.seconds

  final val SecondExecutor: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    (Logging.ignore ++
      ZLayer.succeed(RedisConfig("localhost", 6380)) ++
      ZLayer.succeed[Codec](StringUtf8Codec) >>> RedisExecutor.live).fresh
}
