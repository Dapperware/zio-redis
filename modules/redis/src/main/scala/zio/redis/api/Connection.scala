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

package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.redis.internal.{RedisCommand, RedisEnvironment}

trait Connection[G[+_]] extends RedisEnvironment[G] {
  import Connection.{Auth => _, _}

  /**
   * Authenticates the current connection to the server in two cases:
   *   - If the Redis server is password protected via the ''requirepass'' option
   *   - If a Redis 6.0 instance, or greater, is using the [[https://redis.io/topics/acl Redis ACL system]]. In this
   *     case it is assumed that the implicit username is ''default''.
   *
   * @param password
   *   the password used to authenticate the connection
   * @return
   *   if the password provided via AUTH matches the password in the configuration file, the Unit value is returned and
   *   the server starts accepting commands. Otherwise, an error is returned and the client needs to try a new password.
   */
  final def auth(password: String): G[Unit] = {
    val command = RedisCommand(Connection.Auth, AuthInput, UnitOutput)

    command.run(Auth(None, password))
  }

  /**
   * Authenticates the current connection to the server using username and password.
   *
   * @param username
   *   the username used to authenticate the connection
   * @param password
   *   the password used to authenticate the connection
   * @return
   *   if the password provided via AUTH matches the password in the configuration file, the Unit value is returned and
   *   the server starts accepting commands. Otherwise, an error is returned and the client needs to try a new password.
   */
  final def auth(username: String, password: String): G[Unit] = {
    val command = RedisCommand(Connection.Auth, AuthInput, UnitOutput)

    command.run(Auth(Some(username), password))
  }

  /**
   * Returns the name of the current connection as set by clientSetName
   *
   * @return
   *   the connection name, or None if a name wasn't set.
   */
  final def clientGetName: G[Option[String]] = {
    val command = RedisCommand(ClientGetName, NoInput, OptionalOutput(MultiStringOutput))

    command.run(())
  }

  /**
   * Returns the ID of the current connection. Every connection ID has certain guarantees:
   *   - It is never repeated, so if clientID returns the same number, the caller can be sure that the underlying client
   *     did not disconnect and reconnect the connection, but it is still the same connection.
   *   - The ID is monotonically incremental. If the ID of a connection is greater than the ID of another connection, it
   *     is guaranteed that the second connection was established with the server at a later time.
   *
   * @return
   *   the ID of the current connection.
   */
  final def clientId: G[Long] = {
    val command = RedisCommand(ClientId, NoInput, LongOutput)

    command.run(())
  }

  /**
   * Assigns a name to the current connection
   *
   * @param name
   *   the name to be assigned
   * @return
   *   the Unit value.
   */
  final def clientSetName(name: String): G[Unit] = {
    val command = RedisCommand(ClientSetName, StringInput, UnitOutput)

    command.run(name)
  }

  /**
   * Changes the database for the current connection to the database having the specified numeric index. The currently
   * selected database is a property of the connection; clients should track the selected database and re-select it on
   * reconnection.
   *
   * @param index
   *   the database index. The index is zero-based. New connections always use the database 0
   * @return
   *   the Unit value.
   */
  final def select(index: Long): G[Unit] = {
    val command = RedisCommand(Select, LongInput, UnitOutput)

    command.run(index)
  }
}

private[redis] object Connection {
  final val Auth          = "AUTH"
  final val ClientGetName = "CLIENT GETNAME"
  final val ClientId      = "CLIENT ID"
  final val ClientSetName = "CLIENT SETNAME"
  final val Select        = "SELECT"
}
