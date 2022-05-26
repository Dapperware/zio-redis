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

import zio.config.magnolia.descriptor
import zio.redis.RedisConfig

final case class AppConfig(redis: RedisConfig, server: ServerConfig)

object AppConfig {
  val confDescriptor: _root_.zio.config.ConfigDescriptor[AppConfig] = descriptor[AppConfig]
}