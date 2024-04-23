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

package zio.redis.example.config

import zio.config.magnolia.deriveConfig
import zio.redis.RedisConfig
import zio.{Config, Layer, ZIO, ZLayer}

final case class AppConfig(redis: RedisConfig)

object AppConfig {
  type Env = AppConfig with RedisConfig

  private[this] final val config = ZLayer(ZIO.config(deriveConfig[AppConfig]))

  final val layer: Layer[Config.Error, Env] = config >+> config.project(_.redis)
}
