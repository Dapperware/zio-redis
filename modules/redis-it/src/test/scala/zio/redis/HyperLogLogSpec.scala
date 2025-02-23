package zio.redis

import zio.ZIO
import zio.test.Assertion._
import zio.test._

trait HyperLogLogSpec extends IntegrationSpec {
  def hyperLogLogSuite: Spec[Redis, RedisError] =
    suite("hyperloglog")(
      suite("add elements")(
        test("pfAdd elements to key") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            add   <- redis.pfAdd(key, "one", "two", "three")
          } yield assert(add)(equalTo(true))
        },
        test("pfAdd nothing to key when new elements not unique") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            add1  <- redis.pfAdd(key, "one", "two", "three")
            add2  <- redis.pfAdd(key, "one", "two", "three")
          } yield assert(add1)(equalTo(true)) && assert(add2)(equalTo(false))
        },
        test("pfAdd error when not hyperloglog") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value, None, None)
            add   <- redis.pfAdd(key, "one", "two", "three").either
          } yield assert(add)(isLeft(isSubtype[RedisError.WrongType](anything)))
        }
      ),
      suite("count elements")(
        test("pfCount zero at undefined key") {
          for {
            redis <- ZIO.service[Redis]
            count <- redis.pfCount("noKey")
          } yield assert(count)(equalTo(0L))
        },
        test("pfCount values at key") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            add   <- redis.pfAdd(key, "one", "two", "three")
            count <- redis.pfCount(key)
          } yield assert(add)(equalTo(true)) && assert(count)(equalTo(3L))
        },
        test("pfCount union key with key2") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            key2  <- uuid
            add   <- redis.pfAdd(key, "one", "two", "three")
            add2  <- redis.pfAdd(key2, "four", "five", "six")
            count <- redis.pfCount(key, key2)
          } yield assert(add)(equalTo(true)) && assert(add2)(equalTo(true)) && assert(count)(equalTo(6L))
        } @@ clusterExecutorUnsupported,
        test("error when not hyperloglog") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value, None, None)
            count <- redis.pfCount(key).either
          } yield assert(count)(isLeft)
        }
      ),
      suite("merge")(
        test("pfMerge two hyperloglogs and create destination") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            key2  <- uuid
            key3  <- uuid
            _     <- redis.pfAdd(key, "one", "two", "three", "four")
            _     <- redis.pfAdd(key2, "five", "six", "seven")
            _     <- redis.pfMerge(key3, key2, key)
            count <- redis.pfCount(key3)
          } yield assert(count)(equalTo(7L))
        },
        test("pfMerge two hyperloglogs with already existing destination values") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            key2  <- uuid
            key3  <- uuid
            _     <- redis.pfAdd(key, "one", "two", "three", "four")
            _     <- redis.pfAdd(key2, "five", "six", "seven")
            _     <- redis.pfAdd(key3, "eight", "nine", "ten")
            _     <- redis.pfMerge(key3, key2, key)
            count <- redis.pfCount(key3)
          } yield assert(count)(equalTo(10L))
        },
        test("pfMerge error when source not hyperloglog") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            key2  <- uuid
            key3  <- uuid
            _     <- redis.set(key, value, None, None)
            _     <- redis.pfAdd(key2, "five", "six", "seven")
            merge <- redis.pfMerge(key3, key2, key).either
          } yield assert(merge)(isLeft)
        }
      ) @@ clusterExecutorUnsupported
    )
}
