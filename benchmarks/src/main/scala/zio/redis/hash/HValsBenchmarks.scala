package zio.redis.hash

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{ BenchmarkRuntime, hSet, hVals }

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class HValsBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  var size: Int = _

  private var items: List[(String, String)] = _

  private val key = "test-hash"

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to size).map(e => e.toString -> e.toString).toList
    zioUnsafeRun(hSet(key, items.head, items.tail: _*).unit)
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.implicits.toFoldableOps
    import cats.instances.list._
    unsafeRun[LaserDiscClient](c => items.traverse_(_ => c.send(cmd.hvals(Key.unsafeFrom(key)))))
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c => items.traverse_(_ => RedisCommands.hvals[RedisIO](key).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(_ => c.hVals(key)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(_ => hVals(key).returning[String]))
}
