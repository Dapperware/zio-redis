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

package zio.redis.options

import java.net.InetAddress

trait Connection {
  sealed case class Address(ip: InetAddress, port: Int) {
    private[redis] final def asString: String = s"${ip.getHostAddress}:$port"
  }

  sealed trait UnblockBehavior { self =>
    private[redis] final def asString: String =
      self match {
        case UnblockBehavior.Timeout => "TIMEOUT"
        case UnblockBehavior.Error   => "ERROR"
      }
  }

  object UnblockBehavior {
    case object Timeout extends UnblockBehavior
    case object Error   extends UnblockBehavior
  }

}
