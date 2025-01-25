package zio.redis

import com.dimafeng.testcontainers.DockerComposeContainer
import zio._
import zio.redis.RedisError.ProtocolError
import zio.test.Assertion.{exists => _, _}
import zio.test.TestAspect.{restore => _, _}
import zio.test._

trait KeysSpec extends IntegrationSpec {
  def keysSuite: Spec[DockerComposeContainer & Redis, RedisError] = {
    suite("keys")(
      test("set followed by get") {
        for {
          redis <- ZIO.service[Redis]
          key   <- uuid
          value <- uuid
          _     <- redis.set(key, value)
          v     <- redis.get(key).returning[String]
        } yield assert(v)(isSome(equalTo(value)))
      },
      test("setGet with non-existing key") {
        for {
          redis <- ZIO.service[Redis]
          key   <- uuid
          value <- uuid
          v     <- redis.setGet(key, value)
        } yield assert(v)(isNone)
      },
      test("setGet with the existing key") {
        for {
          redis    <- ZIO.service[Redis]
          value    <- uuid
          key      <- uuid
          _        <- redis.set(key, value)
          newValue <- uuid
          v        <- redis.setGet(key, newValue)
        } yield assert(v)(isSome(equalTo(value)))
      },
      test("get non-existing key") {
        for {
          redis <- ZIO.service[Redis]
          key   <- uuid
          v     <- redis.get(key).returning[String]
        } yield assert(v)(isNone)
      },
      test("handles wrong types") {
        for {
          redis <- ZIO.service[Redis]
          key   <- uuid
          _     <- redis.sAdd(key, "1", "2", "3")
          v     <- redis.get(key).returning[String].either
        } yield assert(v)(isLeft)
      },
      test("check whether or not key exists") {
        for {
          redis <- ZIO.service[Redis]
          key   <- uuid
          value <- uuid
          _     <- redis.set(key, value)
          e1    <- redis.exists(key)
          e2    <- redis.exists("unknown")
        } yield assert(e1)(equalTo(1L)) && assert(e2)(equalTo(0L))
      },
      test("check multiple existence") {
        for {
          redis <- ZIO.service[Redis]
          key   <- uuid
          value <- uuid
          _     <- redis.set(key, value)
          e1    <- redis.exists(key, "unknown")
        } yield assert(e1)(equalTo(1L))
      } @@ clusterExecutorUnsupported,
      test("copy") {
        for {
          redis            <- ZIO.service[Redis]
          sourceKey        <- uuid
          sourceValue      <- uuid
          _                <- redis.set(sourceKey, sourceValue)
          destinationKey   <- uuid
          _                <- redis.copy(sourceKey, destinationKey)
          destinationValue <- redis.get(destinationKey).returning[String]
        } yield assertTrue(destinationValue.contains(sourceValue))
      } @@ clusterExecutorUnsupported,
      test("delete existing key") {
        for {
          redis   <- ZIO.service[Redis]
          key     <- uuid
          value   <- uuid
          _       <- redis.set(key, value)
          deleted <- redis.del(key)
        } yield assert(deleted)(equalTo(1L))
      },
      test("delete multiple existing key") {
        for {
          redis   <- ZIO.service[Redis]
          key1    <- uuid
          key2    <- uuid
          value   <- uuid
          _       <- redis.set(key1, value)
          _       <- redis.set(key2, value)
          deleted <- redis.del(key1, key2)
        } yield assert(deleted)(equalTo(2L))
      } @@ clusterExecutorUnsupported,
      test("find all keys matching pattern") {
        for {
          redis    <- ZIO.service[Redis]
          value    <- uuid
          key1      = "custom_key_1"
          key2      = "custom_key_2"
          _        <- redis.set(key1, value)
          _        <- redis.set(key2, value)
          response <- redis.keys("*custom*").returning[String]
        } yield assert(response)(hasSameElements(Chunk(key1, key2)))
      } @@ clusterExecutorUnsupported,
      test("unlink existing key") {
        for {
          redis   <- ZIO.service[Redis]
          key     <- uuid
          value   <- uuid
          _       <- redis.set(key, value)
          removed <- redis.unlink(key)
        } yield assert(removed)(equalTo(1L))
      } @@ clusterExecutorUnsupported,
      test("touch two existing keys") {
        for {
          redis   <- ZIO.service[Redis]
          key1    <- uuid
          value1  <- uuid
          _       <- redis.set(key1, value1)
          key2    <- uuid
          value2  <- uuid
          _       <- redis.set(key2, value2)
          touched <- redis.touch(key1, key2)
        } yield assert(touched)(equalTo(2L))
      } @@ clusterExecutorUnsupported,
      test("scan entries with match, count and type options")(
        check(genPatternOption, genCountOption, genStringRedisTypeOption) { (pattern, count, redisType) =>
          for {
            redis           <- ZIO.service[Redis]
            key             <- uuid
            value           <- uuid
            _               <- redis.set(key, value)
            scan            <- redis.scan(0L, pattern, count, redisType).returning[String]
            (next, elements) = scan
          } yield assert(next)(isGreaterThanEqualTo(0L)) && assert(elements)(isNonEmpty)
        }
      ) @@ flaky @@ clusterExecutorUnsupported,
      test("fetch random key") {
        for {
          redis     <- ZIO.service[Redis]
          key       <- uuid
          value     <- uuid
          _         <- redis.set(key, value)
          allKeys   <- redis.keys("*").returning[String]
          randomKey <- redis.randomKey.returning[String]
        } yield assert(allKeys)(contains(randomKey.get))
      } @@ ignore @@ clusterExecutorUnsupported,
      test("dump followed by restore") {
        for {
          redis    <- ZIO.service[Redis]
          key      <- uuid
          value    <- uuid
          _        <- redis.set(key, value)
          dumped   <- redis.dump(key)
          _        <- redis.del(key)
          restore  <- redis.restore(key, 0L, dumped).either
          restored <- redis.get(key).returning[String]
        } yield assert(restore)(isRight) && assert(restored)(isSome(equalTo(value)))
      } @@ ignore,
      suite("migrate")(
        test("migrate key to another redis server (copy and replace)") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            value     <- uuid
            _         <- redis.set(key, value)
            response  <- redis
                           .migrate(
                             IntegrationSpec.SingleNode1,
                             6379,
                             key,
                             0L,
                             KeysSpec.MigrateTimeout,
                             copy = Option(Copy),
                             replace = Option(Replace),
                             keys = None
                           )
            originGet <- redis.get(key).returning[String]
            destGet   <- ZIO.serviceWithZIO[Redis](_.get(key).returning[String]).provideLayer(secondExecutor)
          } yield assert(response)(equalTo("OK")) &&
            assert(originGet)(isSome(equalTo(value))) &&
            assert(destGet)(isSome(equalTo(value)))
        },
        test("migrate key to another redis server (move and replace)") {
          for {
            key       <- uuid
            value     <- uuid
            redis     <- ZIO.service[Redis]
            _         <- redis.set(key, value)
            response  <- redis.migrate(
                           IntegrationSpec.SingleNode1,
                           6379L,
                           key,
                           0L,
                           KeysSpec.MigrateTimeout,
                           copy = None,
                           replace = Option(Replace),
                           keys = None
                         )
            originGet <- redis.get(key).returning[String]
            destGet   <- ZIO.serviceWithZIO[Redis](_.get(key).returning[String]).provideLayer(secondExecutor)
          } yield assert(response)(equalTo("OK")) &&
            assert(originGet)(isNone) &&
            assert(destGet)(isSome(equalTo(value)))
        },
        test("migrate key to another redis server (move and no replace, should fail when key exists)") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            _     <- ZIO
                       .serviceWithZIO[Redis](_.set(key, value))
                       .provideLayer(secondExecutor) // also add to second Redis
            response <- redis
                          .migrate(
                            IntegrationSpec.SingleNode1,
                            6379,
                            key,
                            0L,
                            KeysSpec.MigrateTimeout,
                            copy = None,
                            replace = None,
                            keys = None
                          )
                          .either
          } yield assert(response)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("ttl")(
        test("check ttl for existing key") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.pSetEx(key, 1000.millis, value)
            ttl   <- redis.ttl(key).either
          } yield assert(ttl)(isRight)
        } @@ flaky,
        test("check ttl for non-existing key") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            ttl   <- redis.ttl(key).either
          } yield assert(ttl)(isLeft)
        },
        test("check pTtl for existing key") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.pSetEx(key, 1000.millis, value)
            pTtl  <- redis.pTtl(key).either
          } yield assert(pTtl)(isRight)
        } @@ flaky,
        test("check pTtl for non-existing key") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            pTtl  <- redis.pTtl(key).either
          } yield assert(pTtl)(isLeft)
        }
      ),
      suite("expiration")(
        test("set key expiration with pExpire command") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            value     <- uuid
            _         <- redis.set(key, value)
            exp       <- redis.pExpire(key, 2000.millis)
            response1 <- redis.exists(key)
            fiber     <- ZIO.sleep(2050.millis).fork <* TestClock.adjust(2050.millis)
            _         <- fiber.join
            response2 <- redis.exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        },
        test("set key expiration with pExpireAt command") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            value     <- uuid
            expiresAt <- Clock.instant.map(_.plusMillis(2000.millis.toMillis))
            _         <- redis.set(key, value)
            exp       <- redis.pExpireAt(key, expiresAt)
            response1 <- redis.exists(key)
            fiber     <- ZIO.sleep(2050.millis).fork <* TestClock.adjust(2050.millis)
            _         <- fiber.join
            response2 <- redis.exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        },
        test("expire followed by persist") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            value     <- uuid
            _         <- redis.set(key, value)
            exp       <- redis.expire(key, 10000.millis)
            persisted <- redis.persist(key)
          } yield assert(exp)(isTrue) && assert(persisted)(isTrue)
        },
        test("set key expiration with expireAt command") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            value     <- uuid
            expiresAt <- Clock.instant.map(_.plusMillis(2000.millis.toMillis))
            _         <- redis.set(key, value)
            exp       <- redis.expireAt(key, expiresAt)
            response1 <- redis.exists(key)
            fiber     <- ZIO.sleep(2050.millis).fork <* TestClock.adjust(2050.millis)
            _         <- fiber.join
            response2 <- redis.exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(equalTo(1L)) && assert(response2)(equalTo(0L))
        }
      ),
      suite("renaming")(
        test("rename existing key") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- redis.set(key, value)
            renamed <- redis.rename(key, newKey).either
            v       <- redis.get(newKey).returning[String]
          } yield assert(renamed)(isRight) && assert(v)(isSome(equalTo(value)))
        },
        test("try to rename non-existing key") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            newKey  <- uuid
            renamed <- redis.rename(key, newKey).either
          } yield assert(renamed)(isLeft)
        },
        test("rename key to newkey if newkey does not yet exist") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- redis.set(key, value)
            renamed <- redis.renameNx(key, newKey)
            v       <- redis.get(newKey).returning[String]
          } yield assert(renamed)(isTrue) && assert(v)(isSome(equalTo(value)))
        },
        test("try to rename non-existing key with renameNx command") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            newKey  <- uuid
            renamed <- redis.renameNx(key, newKey).either
          } yield assert(renamed)(isLeft)
        }
      ) @@ clusterExecutorUnsupported,
      suite("types")(
        test("string type") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, value)
            string <- redis.typeOf(key)
          } yield assert(string)(equalTo(RedisType.String))
        },
        test("list type") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.lPush(key, value)
            list  <- redis.typeOf(key)
          } yield assert(list)(equalTo(RedisType.List))
        },
        test("set type") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.sAdd(key, value)
            set   <- redis.typeOf(key)
          } yield assert(set)(equalTo(RedisType.Set))
        },
        test("sorted set type") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.zAdd(key)(MemberScore(value, 1d))
            zset  <- redis.typeOf(key)
          } yield assert(zset)(equalTo(RedisType.SortedSet))
        },
        test("hash type") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            field <- uuid
            value <- uuid
            _     <- redis.hSet(key, (field, value))
            hash  <- redis.typeOf(key)
          } yield assert(hash)(equalTo(RedisType.Hash))
        },
        test("stream type") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.xAdd(key, "*", (field, value)).returning[String]
            stream <- redis.typeOf(key)
          } yield assert(stream)(equalTo(RedisType.Stream))
        }
      ),
      suite("sort")(
        test("list of numbers") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.lPush(key, "1", "0", "2")
            sorted <- redis.sort(key).returning[String]
          } yield assert(sorted)(equalTo(Chunk("0", "1", "2")))
        },
        test("list of strings") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.lPush(key, "z", "a", "c")
            sorted <- redis.sort(key, alpha = Some(Alpha)).returning[String]
          } yield assert(sorted)(equalTo(Chunk("a", "c", "z")))
        },
        test("list of numbers, limited") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.lPush(key, "1", "0", "2")
            sorted <- redis.sort(key, limit = Some(Limit(1, 1))).returning[String]
          } yield assert(sorted)(equalTo(Chunk("1")))
        },
        test("descending sort") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.lPush(key, "1", "0", "2")
            sorted <- redis.sort(key, order = Order.Descending).returning[String]
          } yield assert(sorted)(equalTo(Chunk("2", "1", "0")))
        },
        test("by the value referenced by a key-value pair") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            a      <- uuid
            b      <- uuid
            _      <- redis.lPush(key, b, a)
            prefix <- uuid
            _      <- redis.set(s"${prefix}_$a", "A")
            _      <- redis.set(s"${prefix}_$b", "B")
            sorted <- redis.sort(key, by = Some(s"${prefix}_*"), alpha = Some(Alpha)).returning[String]
          } yield assert(sorted)(equalTo(Chunk(a, b)))
        } @@ clusterExecutorUnsupported,
        test("getting the value referenced by a key-value pair") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.lPush(key, value)
            prefix <- uuid
            _      <- redis.set(s"${prefix}_$value", "A")
            sorted <- redis.sort(key, get = Some(s"${prefix}_*" -> List.empty), alpha = Some(Alpha)).returning[String]
          } yield assert(sorted)(equalTo(Chunk("A")))
        } @@ clusterExecutorUnsupported,
        test("getting multiple values referenced by a key-value pair") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            value1  <- uuid
            value2  <- uuid
            _       <- redis.lPush(key, value1, value2)
            prefix  <- uuid
            _       <- redis.set(s"${prefix}_$value1", "A1")
            _       <- redis.set(s"${prefix}_$value2", "A2")
            prefix2 <- uuid
            _       <- redis.set(s"${prefix2}_$value1", "B1")
            _       <- redis.set(s"${prefix2}_$value2", "B2")
            sorted  <- redis
                         .sort(key, get = Some((s"${prefix}_*", List(s"${prefix2}_*"))), alpha = Some(Alpha))
                         .returning[String]
          } yield assert(sorted)(equalTo(Chunk("A1", "B1", "A2", "B2")))
        } @@ flaky @@ clusterExecutorUnsupported,
        test("sort and store result") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            resultKey <- uuid
            _         <- redis.lPush(key, "1", "0", "2")
            count     <- redis.sortStore(key, Store(resultKey))
            sorted    <- redis.lRange(resultKey, 0 to 2).returning[String]
          } yield assert(sorted)(equalTo(Chunk("0", "1", "2"))) && assert(count)(equalTo(3L))
        } @@ clusterExecutorUnsupported
      )
    )
  }

  private val secondExecutor =
    ZLayer
      .makeSome[DockerComposeContainer, Redis](
        singleNodeConfig(IntegrationSpec.SingleNode1),
        ZLayer.succeed[CodecSupplier](ProtobufCodecSupplier),
        Redis.singleNode
      )
}

object KeysSpec {
  final val MigrateTimeout = 5.seconds
}
