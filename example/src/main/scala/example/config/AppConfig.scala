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

package example.config

import com.typesafe.config.ConfigFactory
import zio._
import zio.config.ReadError
import zio.config.magnolia.descriptor
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig
import zio.redis.RedisUri

final case class AppConfig(redis: RedisUri, server: ServerConfig)

object AppConfig {
  type Env = AppConfig with RedisUri

  private[this] final val Config     = ZIO.attempt(ConfigFactory.load.resolve.getConfig("example"))
  private[this] final val Descriptor = descriptor[AppConfig]

  lazy val layer: ZLayer[Any, ReadError[String], Env] =
    TypesafeConfig.fromTypesafeConfig(Config, Descriptor) >+>
      ZLayer.service[AppConfig].narrow(_.redis)
}
