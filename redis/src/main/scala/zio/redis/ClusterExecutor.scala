/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis

import zio._
import zio.redis.ClusterExecutor._
import zio.redis.api.Cluster.AskingCommand
import zio.redis.codec.StringUtf8Codec
import zio.redis.options.Cluster._
import zio.schema.codec.Codec

import java.io.IOException

final case class ClusterExecutor(
  clusterConnectionRef: Ref.Synchronized[ClusterConnection],
  config: RedisClusterConfig,
  scope: Scope.Closeable
) extends RedisExecutor {

  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] = {

    def execute(keySlot: Slot) =
      for {
        executor <- executor(keySlot)
        res      <- executor.execute(command)
      } yield res

    def executeAsk(address: RedisUri) =
      for {
        executor <- executor(address)
        _        <- executor.execute(AskingCommand.resp((), StringUtf8Codec))
        res      <- executor.execute(command)
      } yield res

    def executeSafe(keySlot: Slot) = {
      val recover = execute(keySlot).flatMap {
        case e: RespValue.Error => ZIO.fail(e.toRedisError)
        case success            => ZIO.succeed(success)
      }.catchSome {
        case e: RedisError.Ask   => executeAsk(e.address)
        case _: RedisError.Moved => refreshConnect *> execute(keySlot)
      }
      recover.retry(RetryPolicy)
    }

    for {
      key    <- ZIO.attempt(command(1)).orElseFail(CusterKeyError)
      keySlot = Slot((key.asCRC16 % SlotsAmount).toLong)
      result <- executeSafe(keySlot)
    } yield result
  }

  private def executor(slot: Slot): IO[RedisError.IOError, RedisExecutor] =
    clusterConnectionRef.get.map(_.executor(slot)).flatMap(ZIO.fromOption(_).orElseFail(CusterKeyExecutorError))

  // TODO introduce max connection amount
  private def executor(address: RedisUri): IO[RedisError.IOError, RedisExecutor] =
    clusterConnectionRef.modifyZIO { cc =>
      val executorOpt       = cc.executors.get(address).map(es => (es.executor, cc))
      val enrichedClusterIO = scope.extend(connectToNode(address)).map(es => (es.executor, cc.addExecutor(address, es)))
      ZIO.fromOption(executorOpt).catchAll(_ => enrichedClusterIO)
    }

  private def refreshConnect: IO[RedisError, Unit] =
    clusterConnectionRef.updateZIO { connection =>
      val addresses = connection.partitions.flatMap(_.addresses)
      for {
        cluster <- scope.extend(initConnectToCluster(addresses))
        _       <- ZIO.foreachParDiscard(connection.executors) { case (_, es) => es.scope.close(Exit.unit) }
      } yield cluster
    }

  private val RetryPolicy: Schedule[Any, Throwable, (zio.Duration, Long, Throwable)] =
    Schedule.exponential(config.retry.base, config.retry.factor) &&
      Schedule.recurs(config.retry.maxRecurs) &&
      Schedule.recurWhile[Throwable] {
        case _: RedisError.IOError | _: RedisError.ClusterRedisError => true
        case _                                                       => false
      }
}

object ClusterExecutor {

  lazy val layer: ZLayer[RedisClusterConfig, RedisError, RedisExecutor] =
    ZLayer.scoped {
      for {
        config       <- ZIO.service[RedisClusterConfig]
        layerScope   <- ZIO.scope
        clusterScope <- Scope.make
        executor     <- clusterScope.extend(create(config, clusterScope))
        _            <- layerScope.addFinalizerExit(e => clusterScope.close(e))
      } yield executor
    }

  private[redis] def create(
    config: RedisClusterConfig,
    scope: Scope.Closeable
  ): ZIO[Scope, RedisError, ClusterExecutor] =
    for {
      clusterConnection    <- initConnectToCluster(config.addresses)
      clusterConnectionRef <- Ref.Synchronized.make(clusterConnection)
      clusterExec           = ClusterExecutor(clusterConnectionRef, config, scope)
      _                    <- logScopeFinalizer("Cluster executor is closed")
    } yield clusterExec

  private def initConnectToCluster(addresses: Chunk[RedisUri]): ZIO[Scope, RedisError, ClusterConnection] =
    ZIO
      .collectFirst(addresses) { address =>
        connectToCluster(address).foldZIO(
          error => ZIO.logError(s"The connection to cluster has been failed, $error").as(None),
          cc => ZIO.logInfo("The connection to cluster has been established").as(Some(cc))
        )
      }
      .flatMap(cc => ZIO.getOrFailWith(CusterConnectionError)(cc))

  private def connectToCluster(address: RedisUri) =
    for {
      temporaryRedis    <- redis(address)
      (trLayer, trScope) = temporaryRedis
      partitions        <- slots.provideLayer(trLayer)
      _                 <- ZIO.logTrace(s"Cluster configs:\n${partitions.mkString("\n")}")
      uniqueAddresses    = partitions.map(_.master.address).distinct
      uriExecScope      <- ZIO.foreachPar(uniqueAddresses)(address => connectToNode(address).map(es => address -> es))
      slots              = slotAddress(partitions)
      _                 <- trScope.close(Exit.unit)
    } yield ClusterConnection(partitions, uriExecScope.toMap, slots)

  private def connectToNode(address: RedisUri) =
    for {
      closableScope <- Scope.make
      connection    <- closableScope.extend(RedisConnectionLive.create(RedisConfig(address.host, address.port)))
      executor      <- closableScope.extend(SingleNodeExecutor.create(connection))
      layerScope    <- ZIO.scope
      _             <- layerScope.addFinalizerExit(closableScope.close(_))
    } yield ExecutorScope(executor, closableScope)

  private def redis(address: RedisUri) = {
    val executorLayer = ZLayer.succeed(RedisConfig(address.host, address.port)) >>> RedisExecutor.layer
    val codecLayer    = ZLayer.succeed[Codec](StringUtf8Codec)
    val redisLayer    = executorLayer ++ codecLayer >>> RedisLive.layer
    for {
      closableScope <- Scope.make
      layer         <- closableScope.extend(redisLayer.memoize)
      _             <- logScopeFinalizer("Temporary redis connection is closed")
    } yield (layer, closableScope)
  }

  private def slotAddress(partitions: Chunk[Partition]) =
    partitions.flatMap { p =>
      for (i <- p.slotRange.start to p.slotRange.end) yield Slot(i) -> p.master.address
    }.toMap

  private final val CusterKeyError =
    RedisError.ProtocolError("Key doesn't found. No way to dispatch this command to Redis Cluster")
  private final val CusterKeyExecutorError =
    RedisError.IOError(
      new IOException("Executor for key doesn't found. No way to dispatch this command to Redis Cluster")
    )
  private final val CusterConnectionError =
    RedisError.IOError(new IOException("The connection to cluster has been failed. Can't reach a single startup node."))
}
