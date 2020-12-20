package zio.redis

import zio.clock.Clock
import zio.duration._
import zio.redis.RedisError.ProtocolError
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.{ TestClock, TestConsole, TestRandom, TestSystem }
import zio.{ Chunk, Has, ZIO }

trait KeysSpec extends BaseSpec {

  val keysSuite: Spec[Has[Clock.Service] with Has[RedisExecutor.Service] with Has[TestClock.Service] with Has[
    TestConsole.Service
  ] with Has[TestRandom.Service] with Has[TestSystem.Service] with Has[RedisExecutor.Service] with Has[
    Annotations.Service
  ], TestFailure[RedisError], TestSuccess] = {

    suite("keys")(
      testM("set followed by get") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value)
          v     <- get(key)
        } yield assert(v)(isSome(equalTo(value)))
      },
      testM("get non-existing key") {
        for {
          key <- uuid
          v   <- get(key)
        } yield assert(v)(isNone)
      },
      testM("handles wrong types") {
        for {
          key <- uuid
          _   <- sAdd(key, "1", "2", "3")
          v   <- get(key).either
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
          response <- keys("*custom*")
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
      testM("scan entries") {
        for {
          key             <- uuid
          value           <- uuid
          _               <- set(key, value)
          scan            <- scan(0L)
          (next, elements) = scan
        } yield assert(next)(isGreaterThan(0L)) && assert(elements)(isNonEmpty)
      },
      testM("fetch random key") {
        for {
          key       <- uuid
          value     <- uuid
          _         <- set(key, value)
          allKeys   <- keys("*")
          randomKey <- randomKey()
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
          restored <- get(key)
        } yield assert(restore)(isRight) && assert(restored)(isSome(equalTo(value)))
      },
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
            originGet <- get(key)
            destGet   <- get(key).provideLayer(ApiSpec.secondRedisService)
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
            originGet <- get(key)
            destGet   <- get(key).provideLayer(ApiSpec.secondRedisService)
          } yield assert(response)(equalTo("OK")) &&
            assert(originGet)(isNone) &&
            assert(destGet)(isSome(equalTo(value)))
        },
        testM("migrate key to another redis server (move and no replace, should fail when key exists)") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            _     <- set(key, value).provideLayer(ApiSpec.secondRedisService) // also add to second Redis
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
            v       <- get(newKey)
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
            v       <- get(newKey)
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
            _      <- xAdd(key, "*", (field, value))
            stream <- typeOf(key)
          } yield assert(stream)(equalTo(RedisType.Stream))
        }
      )
    )
  }
}

object KeysSpec {
  final val MigrateTimeout: Duration = 5.seconds
}
