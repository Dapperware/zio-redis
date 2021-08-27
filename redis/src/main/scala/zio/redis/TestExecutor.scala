package zio.redis

import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList
import scala.util.Try

import zio._
import zio.duration._
import zio.redis.RedisError.ProtocolError
import zio.redis.RespValue.{ BulkString, bulkString }
import zio.redis.codec.StringUtf8Codec
import zio.schema.codec.Codec
import zio.stm.{ random => _, _ }

private[redis] final class TestExecutor private (
  lists: TMap[String, Chunk[String]],
  sets: TMap[String, Set[String]],
  strings: TMap[String, String],
  randomPick: Int => USTM[Int],
  hyperLogLogs: TMap[String, Set[String]],
  hashes: TMap[String, Map[String, String]],
  sortedSets: TMap[String, Map[String, Double]]
) extends RedisExecutor.Service {

  override val codec: Codec = StringUtf8Codec

  override def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
    for {
      name <- ZIO.fromOption(command.headOption).orElseFail(ProtocolError("Malformed command."))
      result <- name.asString match {
                  case api.Lists.BlPop =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullArray)

                  case api.Lists.BrPop =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullArray)

                  case api.Lists.BrPopLPush =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString)

                  case api.Lists.BlMove =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString)

                  case api.SortedSets.BzPopMax =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString)

                  case api.SortedSets.BzPopMin =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString)

                  case _ => runCommand(name.asString, command.tail).commit
                }
    } yield result

  private def runBlockingCommand(
    name: String,
    input: Chunk[RespValue.BulkString],
    timeout: Int,
    respValue: RespValue
  ): UIO[RespValue] =
    if (timeout > 0)
      runCommand(name, input).commit
        .timeout(timeout.seconds)
        .map(_.getOrElse(respValue))
        .provideLayer(zio.clock.Clock.live)
    else
      runCommand(name, input).commit

  private def errResponse(cmd: String): RespValue.BulkString =
    RespValue.bulkString(s"(error) ERR wrong number of arguments for '$cmd' command")

  private def onConnection(command: String, input: Chunk[RespValue.BulkString])(
    res: => RespValue.BulkString
  ): USTM[BulkString] = STM.succeedNow(if (input.isEmpty) errResponse(command) else res)

  private[this] def runCommand(name: String, input: Chunk[RespValue.BulkString]): USTM[RespValue] = {
    name match {
      case api.Connection.Ping =>
        STM.succeedNow {
          if (input.isEmpty)
            RespValue.bulkString("PONG")
          else
            input.head
        }

      case api.Connection.Auth =>
        onConnection(name, input)(RespValue.bulkString("OK"))

      case api.Connection.Echo =>
        onConnection(name, input)(input.head)

      case api.Connection.Select =>
        onConnection(name, input)(RespValue.bulkString("OK"))

      case api.Sets.SAdd =>
        val key = input.head.asString
        orWrongType(isSet(key))(
          {
            val values = input.tail.map(_.asString)
            for {
              oldSet <- sets.getOrElse(key, Set.empty)
              newSet  = oldSet ++ values
              added   = newSet.size - oldSet.size
              _      <- sets.put(key, newSet)
            } yield RespValue.Integer(added.toLong)
          }
        )

      case api.Sets.SCard =>
        val key = input.head.asString

        orWrongType(isSet(key))(
          sets.get(key).map(_.fold(RespValue.Integer(0))(s => RespValue.Integer(s.size.toLong)))
        )

      case api.Sets.SDiff =>
        val allkeys = input.map(_.asString)
        val mainKey = allkeys.head
        val others  = allkeys.tail

        orWrongType(forAll(allkeys)(isSet))(
          for {
            main   <- sets.getOrElse(mainKey, Set.empty)
            result <- STM.foldLeft(others)(main) { case (acc, k) => sets.get(k).map(_.fold(acc)(acc -- _)) }
          } yield RespValue.array(result.map(RespValue.bulkString).toList: _*)
        )

      case api.Sets.SDiffStore =>
        val allkeys = input.map(_.asString)
        val distkey = allkeys.head
        val mainKey = allkeys(1)
        val others  = allkeys.drop(2)

        orWrongType(forAll(allkeys)(isSet))(
          for {
            main   <- sets.getOrElse(mainKey, Set.empty)
            result <- STM.foldLeft(others)(main) { case (acc, k) => sets.get(k).map(_.fold(acc)(acc -- _)) }
            _      <- sets.put(distkey, result)
          } yield RespValue.Integer(result.size.toLong)
        )

      case api.Sets.SInter =>
        val keys      = input.map(_.asString)
        val mainKey   = keys.head
        val otherKeys = keys.tail
        sInter(mainKey, otherKeys).fold(_ => Replies.WrongType, Replies.array)

      case api.Sets.SInterStore =>
        val keys        = input.map(_.asString)
        val destination = keys.head
        val mainKey     = keys(1)
        val otherKeys   = keys.tail

        (STM.fail(()).unlessM(isSet(destination)) *> sInter(mainKey, otherKeys)).foldM(
          _ => STM.succeedNow(Replies.WrongType),
          s =>
            for {
              _ <- sets.put(destination, s)
            } yield RespValue.Integer(s.size.toLong)
        )

      case api.Sets.SIsMember =>
        val key    = input.head.asString
        val member = input(1).asString

        orWrongType(isSet(key))(
          for {
            set   <- sets.getOrElse(key, Set.empty)
            result = if (set.contains(member)) RespValue.Integer(1) else RespValue.Integer(0)
          } yield result
        )

      case api.Sets.SMove =>
        val sourceKey      = input.head.asString
        val destinationKey = input(1).asString
        val member         = input(2).asString

        orWrongType(isSet(sourceKey))(
          sets.getOrElse(sourceKey, Set.empty).flatMap { source =>
            if (source.contains(member))
              STM.ifM(isSet(destinationKey))(
                for {
                  dest <- sets.getOrElse(destinationKey, Set.empty)
                  _    <- sets.put(sourceKey, source - member)
                  _    <- sets.put(destinationKey, dest + member)
                } yield RespValue.Integer(1),
                STM.succeedNow(Replies.WrongType)
              )
            else STM.succeedNow(RespValue.Integer(0))
          }
        )

      case api.Sets.SPop =>
        val key   = input.head.asString
        val count = if (input.size == 1) 1 else input(1).asString.toInt

        orWrongType(isSet(key))(
          for {
            set   <- sets.getOrElse(key, Set.empty)
            result = set.take(count)
            _     <- sets.put(key, set -- result)
          } yield Replies.array(result)
        )

      case api.Sets.SMembers =>
        val key = input.head.asString

        orWrongType(isSet(key))(
          sets.get(key).map(_.fold(Replies.EmptyArray)(Replies.array(_)))
        )

      case api.Sets.SRandMember =>
        val key = input.head.asString

        orWrongType(isSet(key))(
          {
            val maybeCount = input.tail.headOption.map(b => b.asString.toLong)
            for {
              set     <- sets.getOrElse(key, Set.empty[String])
              asVector = set.toVector
              res <- maybeCount match {
                       case None =>
                         selectOne[String](asVector, randomPick).map {
                           _.fold(RespValue.NullBulkString: RespValue)(RespValue.bulkString)
                         }
                       case Some(n) if n > 0 => selectN(asVector, n, randomPick).map(Replies.array)
                       case Some(n) if n < 0 => selectNWithReplacement(asVector, -n, randomPick).map(Replies.array)
                       case Some(_)          => STM.succeedNow(RespValue.NullBulkString)
                     }
            } yield res
          }
        )

      case api.Sets.SRem =>
        val key = input.head.asString

        orWrongType(isSet(key))(
          {
            val values = input.tail.map(_.asString)
            for {
              oldSet <- sets.getOrElse(key, Set.empty)
              newSet  = oldSet -- values
              removed = oldSet.size - newSet.size
              _      <- sets.put(key, newSet)
            } yield RespValue.Integer(removed.toLong)
          }
        )

      case api.Sets.SUnion =>
        val keys = input.map(_.asString)

        orWrongType(forAll(keys)(isSet))(
          STM
            .foldLeft(keys)(Set.empty[String]) { (unionSoFar, nextKey) =>
              sets.getOrElse(nextKey, Set.empty[String]).map { currentSet =>
                unionSoFar ++ currentSet
              }
            }
            .map(unionSet => Replies.array(unionSet))
        )

      case api.Sets.SUnionStore =>
        val destination = input.head.asString
        val keys        = input.tail.map(_.asString)

        orWrongType(forAll(keys)(isSet))(
          for {
            union <- STM
                       .foldLeft(keys)(Set.empty[String]) { (unionSoFar, nextKey) =>
                         sets.getOrElse(nextKey, Set.empty[String]).map { currentSet =>
                           unionSoFar ++ currentSet
                         }
                       }
            _ <- sets.put(destination, union)
          } yield RespValue.Integer(union.size.toLong)
        )

      case api.Sets.SScan =>
        val key   = input.head.asString
        val start = input(1).asString.toInt

        val maybeRegex =
          if (input.size > 2) input(2).asString match {
            case "MATCH" => Some(input(3).asString.replace("*", ".*").r)
            case _       => None
          }
          else None

        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val maybeCount =
          if (input.size > 4) maybeGetCount(input(4), input(5))
          else if (input.size > 2) maybeGetCount(input(2), input(3))
          else None

        val end = start + maybeCount.getOrElse(10)

        orWrongType(isSet(key))(
          {
            for {
              set      <- sets.getOrElse(key, Set.empty)
              filtered  = maybeRegex.map(regex => set.filter(s => regex.pattern.matcher(s).matches)).getOrElse(set)
              resultSet = filtered.slice(start, end)
              nextIndex = if (filtered.size <= end) 0 else end
              results   = Replies.array(resultSet)
            } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
          }
        )

      case api.Strings.Set =>
        // not a full implementation. Just enough to make set tests work
        val key   = input.head.asString
        val value = input(1).asString
        strings.put(key, value).as(Replies.Ok)

      case api.HyperLogLog.PfAdd =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString).toSet

        orWrongType(isHyperLogLog(key))(
          for {
            oldValues <- hyperLogLogs.getOrElse(key, Set.empty)
            ret        = if (oldValues == values) 0L else 1L
            _         <- hyperLogLogs.put(key, values)
          } yield RespValue.Integer(ret)
        )

      case api.HyperLogLog.PfCount =>
        val keys = input.map(_.asString)
        orWrongType(forAll(keys)(isHyperLogLog))(
          STM
            .foldLeft(keys)(Set.empty[String]) { (bHyperLogLogs, nextKey) =>
              hyperLogLogs.getOrElse(nextKey, Set.empty[String]).map { currentSet =>
                bHyperLogLogs ++ currentSet
              }
            }
            .map(vs => RespValue.Integer(vs.size.toLong))
        )

      case api.HyperLogLog.PfMerge =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(forAll(values ++ Chunk.single(key))(isHyperLogLog))(
          for {
            sourceValues <- STM.foldLeft(values)(Set.empty: Set[String]) { (bHyperLogLogs, nextKey) =>
                              hyperLogLogs.getOrElse(nextKey, Set.empty).map { currentSet =>
                                bHyperLogLogs ++ currentSet
                              }
                            }
            destValues <- hyperLogLogs.getOrElse(key, Set.empty)
            putValues   = sourceValues ++ destValues
            _          <- hyperLogLogs.put(key, putValues)
          } yield Replies.Ok
        )

      case api.Lists.LIndex =>
        val key   = input.head.asString
        val index = input(1).asString.toInt

        orWrongType(isList(key))(
          for {
            list  <- lists.getOrElse(key, Chunk.empty)
            result = if (index < 0) list.lift(list.size + index) else list.lift(index)
          } yield result.fold[RespValue](RespValue.NullBulkString)(value => RespValue.bulkString(value))
        )

      case api.Lists.LInsert =>
        val key = input.head.asString
        val position = input(1).asString match {
          case "BEFORE" => Position.Before
          case "AFTER"  => Position.After
        }
        val pivot   = input(2).asString
        val element = input(3).asString

        orWrongType(isList(key))(
          for {
            maybeList <- lists.get(key)
            eitherResult = maybeList match {
                             case None => Left(RespValue.Integer(0L))
                             case Some(list) =>
                               Right(
                                 position match {
                                   case Position.Before =>
                                     list.find(_ == pivot).map { p =>
                                       val index        = list.indexOf(p)
                                       val (head, tail) = list.splitAt(index)
                                       head ++ Chunk.single(element) ++ tail
                                     }

                                   case Position.After =>
                                     list.find(_ == pivot).map { p =>
                                       val index        = list.indexOf(p)
                                       val (head, tail) = list.splitAt(index + 1)
                                       head ++ Chunk.single(element) ++ tail
                                     }
                                 }
                               )
                           }
            result <- eitherResult.fold(
                        respValue => STM.succeedNow(respValue),
                        maybeList =>
                          maybeList.fold(STM.succeedNow(RespValue.Integer(-1L)))(insert =>
                            lists.put(key, insert) *> STM.succeedNow(RespValue.Integer(insert.size.toLong))
                          )
                      )
          } yield result
        )

      case api.Lists.LLen =>
        val key = input.head.asString

        orWrongType(isList(key))(
          for {
            list <- lists.getOrElse(key, Chunk.empty)
          } yield RespValue.Integer(list.size.toLong)
        )

      case api.Lists.LPush =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(isList(key))(
          for {
            oldValues <- lists.getOrElse(key, Chunk.empty)
            newValues  = values.reverse ++ oldValues
            _         <- lists.put(key, newValues)
          } yield RespValue.Integer(newValues.size.toLong)
        )

      case api.Lists.LPushX =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(isList(key))(
          (for {
            list    <- lists.get(key)
            newList <- STM.fromOption(list.map(oldValues => values.reverse ++ oldValues))
            _       <- lists.put(key, newList)
          } yield newList).fold(_ => RespValue.Integer(0L), result => RespValue.Integer(result.size.toLong))
        )

      case api.Lists.LRange =>
        val key   = input.head.asString
        val start = input(1).asString.toInt
        val end   = input(2).asString.toInt

        orWrongType(isList(key))(
          for {
            list  <- lists.getOrElse(key, Chunk.empty)
            result = if (end < 0) list.slice(start, list.size + 1 + end) else list.slice(start, end + 1)
          } yield Replies.array(result)
        )

      case api.Lists.LRem =>
        val key     = input.head.asString
        val count   = input(1).asString.toInt
        val element = input(2).asString

        orWrongType(isList(key))(
          for {
            list <- lists.getOrElse(key, Chunk.empty)
            result = count match {
                       case 0          => list.filterNot(_ == element)
                       case n if n > 0 => dropWhileLimit(list)(_ == element, n)
                       case n if n < 0 => dropWhileLimit(list.reverse)(_ == element, n * (-1)).reverse
                     }
            _ <- lists.put(key, result)
          } yield RespValue.Integer((list.size - result.size).toLong)
        )

      case api.Lists.LSet =>
        val key     = input.head.asString
        val index   = input(1).asString.toInt
        val element = input(2).asString

        orWrongType(isList(key))(
          (for {
            list <- lists.getOrElse(key, Chunk.empty)
            newList <- STM.fromOption {
                         Try {
                           if (index < 0) list.updated(list.size - 1 + index, element)
                           else list.updated(index, element)
                         }.toOption
                       }
            _ <- lists.put(key, newList)
          } yield ()).fold(_ => RespValue.Error("ERR index out of range"), _ => RespValue.SimpleString("OK"))
        )

      case api.Lists.LTrim =>
        val key   = input.head.asString
        val start = input(1).asString.toInt
        val stop  = input(2).asString.toInt

        orWrongType(isList(key))(
          for {
            list <- lists.getOrElse(key, Chunk.empty)
            result = (start, stop) match {
                       case (l, r) if l >= 0 && r >= 0 => list.slice(l, r + 1)
                       case (l, r) if l < 0 && r >= 0  => list.slice(list.size + l, r + 1)
                       case (l, r) if l >= 0 && r < 0  => list.slice(l, list.size + r + 1)
                       case (l, r) if l < 0 && r < 0   => list.slice(list.size + l, list.size + r + 1)
                       case (_, _)                     => list
                     }
            _ <- lists.put(key, result)
          } yield RespValue.SimpleString("OK")
        )

      case api.Lists.RPop =>
        val key   = input.head.asString
        val count = if (input.size == 1) 1 else input(1).asString.toInt

        orWrongType(isList(key))(
          for {
            list  <- lists.getOrElse(key, Chunk.empty)
            result = list.takeRight(count).reverse
            _     <- lists.put(key, list.dropRight(count))
          } yield result.size match {
            case 0 => RespValue.NullBulkString
            case 1 => RespValue.bulkString(result.head)
            case _ => Replies.array(result)
          }
        )

      case api.Lists.RPopLPush =>
        val source      = input(0).asString
        val destination = input(1).asString

        orWrongType(forAll(Chunk(source, destination))(isList))(
          for {
            sourceList       <- lists.getOrElse(source, Chunk.empty)
            destinationList  <- lists.getOrElse(destination, Chunk.empty)
            value             = sourceList.lastOption
            sourceResult      = sourceList.dropRight(1)
            destinationResult = value.map(_ +: destinationList).getOrElse(destinationList)
            _                <- lists.put(source, sourceResult)
            _                <- lists.put(destination, destinationResult)
          } yield value.fold[RespValue](RespValue.NullBulkString)(result => RespValue.bulkString(result))
        )

      case api.Lists.RPush =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(isList(key))(
          for {
            oldValues <- lists.getOrElse(key, Chunk.empty)
            newValues  = values ++ oldValues
            _         <- lists.put(key, newValues)
          } yield RespValue.Integer(newValues.size.toLong)
        )

      case api.Lists.LPop =>
        val key   = input.head.asString
        val count = if (input.size == 1) 1 else input(1).asString.toInt

        orWrongType(isList(key))(
          for {
            list  <- lists.getOrElse(key, Chunk.empty)
            result = list.take(count)
            _     <- lists.put(key, list.drop(count))
          } yield result.size match {
            case 0 => RespValue.NullBulkString
            case 1 => RespValue.bulkString(result.head)
            case _ => Replies.array(result)
          }
        )

      case api.Lists.RPushX =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(isList(key))(
          (for {
            list    <- lists.get(key)
            newList <- STM.fromOption(list.map(oldValues => oldValues ++ values.toVector))
            _       <- lists.put(key, newList)
          } yield newList).fold(_ => RespValue.Integer(0L), result => RespValue.Integer(result.size.toLong))
        )

      case api.Lists.BlPop =>
        val keys = input.dropRight(1).map(_.asString)

        orWrongType(forAll(keys)(isList))(
          (for {
            allLists <-
              STM.foreach(keys.map(key => STM.succeedNow(key) &&& lists.getOrElse(key, Chunk.empty)))(identity)
            nonEmptyLists <- STM.succeed(allLists.collect { case (key, v) if v.nonEmpty => key -> v })
            (sk, sl)      <- STM.fromOption(nonEmptyLists.headOption)
            _             <- lists.put(sk, sl.tail)
          } yield Replies.array(Chunk(sk, sl.head))).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.Lists.BrPop =>
        val keys = input.dropRight(1).map(_.asString)

        orWrongType(forAll(keys)(isList))(
          (for {
            allLists <-
              STM.foreach(keys.map(key => STM.succeedNow(key) &&& lists.getOrElse(key, Chunk.empty)))(identity)
            nonEmptyLists <- STM.succeed(allLists.collect { case (key, v) if v.nonEmpty => key -> v })
            (sk, sl)      <- STM.fromOption(nonEmptyLists.headOption)
            _             <- lists.put(sk, sl.dropRight(1))
          } yield Replies.array(Chunk(sk, sl.last))).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.Lists.BrPopLPush =>
        val source      = input.head.asString
        val destination = input.tail.head.asString

        orWrongType(forAll(Chunk(source, destination))(isList))(
          (for {
            sourceListOpt    <- lists.get(source)
            sourceList       <- STM.fromOption(sourceListOpt)
            destinationList  <- lists.getOrElse(destination, Chunk.empty)
            value            <- STM.fromOption(sourceList.lastOption)
            sourceResult      = sourceList.dropRight(1)
            destinationResult = value +: destinationList
            _                <- lists.put(source, sourceResult)
            _                <- lists.put(destination, destinationResult)
          } yield RespValue.bulkString(value)).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.Lists.LMove =>
        val source      = input(0).asString
        val destination = input(1).asString
        val sourceSide = input(2).asString match {
          case "LEFT"  => Side.Left
          case "RIGHT" => Side.Right
        }
        val destinationSide = input(3).asString match {
          case "LEFT"  => Side.Left
          case "RIGHT" => Side.Right
        }

        orWrongType(forAll(Chunk(source, destination))(isList))(
          (for {
            sourceList      <- lists.get(source) >>= (l => STM.fromOption(l))
            destinationList <- lists.getOrElse(destination, Chunk.empty)
            element <- STM.fromOption(sourceSide match {
                         case Side.Left  => sourceList.headOption
                         case Side.Right => sourceList.lastOption
                       })
            newSourceList = sourceSide match {
                              case Side.Left  => sourceList.drop(1)
                              case Side.Right => sourceList.dropRight(1)
                            }
            newDestinationList =
              if (source != destination)
                destinationSide match {
                  case Side.Left  => element +: destinationList
                  case Side.Right => destinationList :+ element
                }
              else
                destinationSide match {
                  case Side.Left  => element +: newSourceList
                  case Side.Right => newSourceList :+ element
                }
            _ <- if (source != destination)
                   lists.put(source, newSourceList) *> lists.put(destination, newDestinationList)
                 else
                   lists.put(source, newDestinationList)
          } yield element).fold(_ => RespValue.NullBulkString, result => RespValue.bulkString(result))
        )

      case api.Lists.BlMove =>
        val source      = input(0).asString
        val destination = input(1).asString
        val sourceSide = input(2).asString match {
          case "LEFT"  => Side.Left
          case "RIGHT" => Side.Right
        }
        val destinationSide = input(3).asString match {
          case "LEFT"  => Side.Left
          case "RIGHT" => Side.Right
        }

        orWrongType(forAll(Chunk(source, destination))(isList))(
          (for {
            sourceList      <- lists.get(source) >>= (l => STM.fromOption(l))
            destinationList <- lists.getOrElse(destination, Chunk.empty)
            element <- STM.fromOption(sourceSide match {
                         case Side.Left  => sourceList.headOption
                         case Side.Right => sourceList.lastOption
                       })
            newSourceList = sourceSide match {
                              case Side.Left  => sourceList.drop(1)
                              case Side.Right => sourceList.dropRight(1)
                            }
            newDestinationList =
              if (source != destination)
                destinationSide match {
                  case Side.Left  => element +: destinationList
                  case Side.Right => destinationList :+ element
                }
              else
                destinationSide match {
                  case Side.Left  => element +: newSourceList
                  case Side.Right => newSourceList :+ element
                }
            _ <- if (source != destination)
                   lists.put(source, newSourceList) *> lists.put(destination, newDestinationList)
                 else
                   lists.put(source, newDestinationList)
          } yield element).foldM(_ => STM.retry, result => STM.succeed(RespValue.bulkString(result)))
        )

      case api.Lists.LPos =>
        val key     = input(0).asString
        val element = input(1).asString

        val options      = input.map(_.asString).zipWithIndex
        val rankOption   = options.find(_._1 == "RANK").map(_._2).map(idx => input(idx + 1).asLong)
        val countOption  = options.find(_._1 == "COUNT").map(_._2).map(idx => input(idx + 1).asLong)
        val maxLenOption = options.find(_._1 == "MAXLEN").map(_._2).map(idx => input(idx + 1).asLong)

        orWrongType(isList(key))(
          for {
            list <- lists.getOrElse(key, Chunk.empty).map { list =>
                      (maxLenOption, rankOption) match {
                        case (Some(maxLen), None)                   => list.take(maxLen.toInt)
                        case (Some(maxLen), Some(rank)) if rank < 0 => list.reverse.take(maxLen.toInt).reverse
                        case (_, _)                                 => list
                      }
                    }
            idxList = list.zipWithIndex
            result = (countOption, rankOption) match {
                       case (Some(count), Some(rank)) if rank < 0 =>
                         Right(
                           idxList.reverse
                             .filter(_._1 == element)
                             .drop((rank.toInt * -1) - 1)
                             .take(count.toInt)
                             .map(_._2)
                         )
                       case (Some(count), Some(rank)) =>
                         Right(
                           idxList
                             .filter(_._1 == element)
                             .drop(rank.toInt - 1)
                             .take(count.toInt)
                             .map(_._2)
                         )
                       case (Some(count), None) =>
                         Right(
                           idxList
                             .filter(_._1 == element)
                             .take(count.toInt)
                             .map(_._2)
                         )
                       case (None, Some(rank)) if rank < 0 =>
                         Left(
                           idxList.reverse
                             .filter(_._1 == element)
                             .drop(rank.toInt)
                             .map(_._2)
                             .headOption
                         )
                       case (None, Some(rank)) =>
                         Left(
                           idxList
                             .filter(_._1 == element)
                             .drop(rank.toInt - 1)
                             .map(_._2)
                             .headOption
                         )
                       case (None, None) =>
                         Left(
                           idxList
                             .filter(_._1 == element)
                             .map(_._2)
                             .headOption
                         )
                     }

          } yield result.fold(
            left => left.fold[RespValue](RespValue.NullBulkString)(value => RespValue.Integer(value.toLong)),
            right => RespValue.array(right.map(value => RespValue.Integer(value.toLong)): _*)
          )
        )

      case api.Hashes.HDel =>
        val key    = input(0).asString
        val values = input.tail.map(_.asString)

        orWrongType(isHash(key))(
          for {
            hash       <- hashes.getOrElse(key, Map.empty)
            countExists = hash.keys count values.contains
            newHash     = hash -- values
            _          <- if (newHash.isEmpty) hashes.delete(key) else hashes.put(key, newHash)
          } yield RespValue.Integer(countExists.toLong)
        )

      case api.Hashes.HExists =>
        val key   = input(0).asString
        val field = input(1).asString

        orWrongType(isHash(key))(
          for {
            hash  <- hashes.getOrElse(key, Map.empty)
            exists = hash.keys.exists(_ == field)
          } yield if (exists) RespValue.Integer(1L) else RespValue.Integer(0L)
        )

      case api.Hashes.HGet =>
        val key   = input(0).asString
        val field = input(1).asString

        orWrongType(isHash(key))(
          for {
            hash <- hashes.getOrElse(key, Map.empty)
            value = hash.get(field)
          } yield value.fold[RespValue](RespValue.NullBulkString)(result => RespValue.bulkString(result))
        )

      case api.Hashes.HGetAll =>
        val key = input(0).asString

        orWrongType(isHash(key))(
          for {
            hash   <- hashes.getOrElse(key, Map.empty)
            results = hash.flatMap { case (k, v) => Iterable.apply(k, v) } map RespValue.bulkString
          } yield RespValue.Array(Chunk.fromIterable(results))
        )

      case api.Hashes.HIncrBy =>
        val key   = input(0).asString
        val field = input(1).asString
        val incr  = input(2).asString.toLong

        orWrongType(isHash(key))(
          (for {
            hash     <- hashes.getOrElse(key, Map.empty)
            newValue <- STM.fromTry(Try(hash.getOrElse(field, "0").toLong + incr))
            newMap    = hash + (field -> newValue.toString)
            _        <- hashes.put(key, newMap)
          } yield newValue).fold(_ => Replies.Error, result => RespValue.Integer(result))
        )

      case api.Hashes.HIncrByFloat =>
        val key   = input(0).asString
        val field = input(1).asString
        val incr  = input(2).asString.toDouble

        orWrongType(isHash(key))(
          (for {
            hash     <- hashes.getOrElse(key, Map.empty)
            newValue <- STM.fromTry(Try(hash.getOrElse(field, "0").toDouble + incr))
            newHash   = hash + (field -> newValue.toString)
            _        <- hashes.put(key, newHash)
          } yield newValue).fold(_ => Replies.Error, result => RespValue.bulkString(result.toString))
        )

      case api.Hashes.HKeys =>
        val key = input(0).asString

        orWrongType(isHash(key))(
          for {
            hash <- hashes.getOrElse(key, Map.empty)
          } yield RespValue.Array(Chunk.fromIterable(hash.keys map RespValue.bulkString))
        )

      case api.Hashes.HLen =>
        val key = input(0).asString

        orWrongType(isHash(key))(
          for {
            hash <- hashes.getOrElse(key, Map.empty)
          } yield RespValue.Integer(hash.size.toLong)
        )

      case api.Hashes.HmGet =>
        val key    = input(0).asString
        val fields = input.tail.map(_.asString)

        orWrongType(isHash(key))(
          for {
            hash  <- hashes.getOrElse(key, Map.empty)
            result = fields.map(hash.get)
          } yield RespValue.Array(result.map {
            case None        => RespValue.NullBulkString
            case Some(value) => RespValue.bulkString(value)
          })
        )

      case api.Hashes.HmSet =>
        val key    = input(0).asString
        val values = input.tail.map(_.asString)

        orWrongType(isHash(key))(
          for {
            hash  <- hashes.getOrElse(key, Map.empty)
            newMap = hash ++ values.grouped(2).map(g => (g(0), g(1)))
            _     <- hashes.put(key, newMap)
          } yield Replies.Ok
        )

      case api.Hashes.HScan =>
        val key   = input.head.asString
        val start = input(1).asString.toInt

        val maybeRegex =
          if (input.size > 2)
            input(2).asString match {
              case "MATCH" => Some(input(3).asString.replace("*", ".*").r)
              case _       => None
            }
          else None

        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val maybeCount =
          if (input.size > 4) maybeGetCount(input(4), input(5))
          else if (input.size > 2) maybeGetCount(input(2), input(3))
          else None

        val end = start + maybeCount.getOrElse(10)

        orWrongType(isHash(key))(
          for {
            set <- hashes.getOrElse(key, Map.empty)
            filtered =
              maybeRegex.map(regex => set.filter { case (k, _) => regex.pattern.matcher(k).matches }).getOrElse(set)
            resultSet = filtered.slice(start, end)
            nextIndex = if (filtered.size <= end) 0 else end
            results   = Replies.array(resultSet.flatMap { case (k, v) => Iterable(k, v) })
          } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
        )

      case api.Hashes.HSet =>
        val key    = input(0).asString
        val values = input.tail.map(_.asString)

        orWrongType(isHash(key))(
          for {
            hash   <- hashes.getOrElse(key, Map.empty)
            newHash = hash ++ values.grouped(2).map(g => (g(0), g(1)))
            _      <- hashes.put(key, newHash)
          } yield RespValue.Integer(newHash.size.toLong - hash.size.toLong)
        )

      case api.Hashes.HSetNx =>
        val key   = input(0).asString
        val field = input(1).asString
        val value = input(2).asString

        orWrongType(isHash(key))(
          for {
            hash    <- hashes.getOrElse(key, Map.empty)
            contains = hash.contains(field)
            newHash  = hash ++ (if (contains) Map.empty else Map(field -> value))
            _       <- hashes.put(key, newHash)
          } yield RespValue.Integer(if (contains) 0L else 1L)
        )

      case api.Hashes.HStrLen =>
        val key   = input(0).asString
        val field = input(1).asString

        orWrongType(isHash(key))(
          for {
            hash <- hashes.getOrElse(key, Map.empty)
            len   = hash.get(field).map(_.length.toLong).getOrElse(0L)
          } yield RespValue.Integer(len)
        )

      case api.Hashes.HVals =>
        val key = input(0).asString

        orWrongType(isHash(key))(
          for {
            hash  <- hashes.getOrElse(key, Map.empty)
            values = hash.values map RespValue.bulkString
          } yield RespValue.Array(Chunk.fromIterable(values))
        )

      case api.Hashes.HRandField =>
        val key        = input(0).asString
        val count      = if (input.size == 1) None else Some(input(1).asString.toLong)
        val withValues = if (input.size == 3) input(2).asString == "WITHVALUES" else false

        def selectValues[T](n: Long, values: Vector[T]) = {
          val repeatedAllowed = n < 0
          val count           = Math.abs(n)
          val t               = count - values.length

          if (repeatedAllowed && t > 0) {
            selectNWithReplacement[T](values, count, randomPick)
          } else {
            selectN[T](values, count, randomPick)
          }
        }

        orWrongType(isHash(key)) {
          val keysAndValues = for {
            hash <- hashes.getOrElse(key, Map.empty)
          } yield (hash.keys map RespValue.bulkString) zip (hash.values map RespValue.bulkString)

          if (count.isDefined && withValues) {
            for {
              kvs            <- keysAndValues
              fields         <- selectValues(count.get, kvs.toVector)
              fieldsAndValues = fields.flatMap { case (k, v) => Seq(k, v) }
            } yield RespValue.Array(Chunk.fromIterable(fieldsAndValues))
          } else if (count.isDefined) {
            for {
              kvs    <- keysAndValues
              keys    = kvs.map(_._1)
              fields <- selectValues(count.get, keys.toVector)
            } yield RespValue.Array(Chunk.fromIterable(fields))
          } else {
            for {
              kvs <- keysAndValues
              keys = kvs.map { case (k, _) => k }
              key <- selectOne[zio.redis.RespValue.BulkString](keys.toVector, randomPick)
            } yield key.getOrElse(RespValue.NullBulkString)
          }
        }

      case api.SortedSets.BzPopMax =>
        val keys = input.dropRight(1).map(_.asString)

        orWrongType(forAll(keys)(isSortedSet))(
          (for {
            allSets <-
              STM.foreach(keys.map(key => STM.succeedNow(key) &&& sortedSets.getOrElse(key, Map.empty)))(identity)
            nonEmpty    <- STM.succeed(allSets.collect { case (key, v) if v.nonEmpty => key -> v })
            (sk, sl)    <- STM.fromOption(nonEmpty.headOption)
            (maxM, maxV) = sl.toList.maxBy(_._2)
            _           <- sortedSets.put(sk, sl - maxM)
          } yield Replies.array(Chunk(sk, maxM, maxV.toString))).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.SortedSets.BzPopMin =>
        val keys = input.dropRight(1).map(_.asString)

        orWrongType(forAll(keys)(isSortedSet))(
          (for {
            allSets <-
              STM.foreach(keys.map(key => STM.succeedNow(key) &&& sortedSets.getOrElse(key, Map.empty)))(identity)
            nonEmpty    <- STM.succeed(allSets.collect { case (key, v) if v.nonEmpty => key -> v })
            (sk, sl)    <- STM.fromOption(nonEmpty.headOption)
            (maxM, maxV) = sl.toList.minBy(_._2)
            _           <- sortedSets.put(sk, sl - maxM)
          } yield Replies.array(Chunk(sk, maxM, maxV.toString))).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.SortedSets.ZAdd =>
        val key = input(0).asString

        val updateOption = input.map(_.asString).find {
          case "XX" => true
          case "NX" => true
          case "LT" => true
          case "GT" => true
          case _    => false
        }

        val changedOption = input.map(_.asString).find {
          case "CH" => true
          case _    => false
        }

        val incrOption = input.map(_.asString).find {
          case "INCR" => true
          case _      => false
        }

        val optionsCount = updateOption.map(_ => 1).getOrElse(0) + changedOption.map(_ => 1).getOrElse(0) + incrOption
          .map(_ => 1)
          .getOrElse(0)

        val values =
          Chunk.fromIterator(
            input
              .drop(1 + optionsCount)
              .map(_.asString)
              .grouped(2)
              .map(g => MemberScore(g(0).toDouble, g(1)))
          )

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            valuesToAdd = updateOption.map {
                            case "XX" =>
                              values.filter(ms => scoreMap.contains(ms.member))

                            case "NX" =>
                              values.filter(ms => !scoreMap.contains(ms.member))

                            case "LT" =>
                              values.filter(ms =>
                                scoreMap.exists { case (member, score) =>
                                  (member == ms.member && score > ms.score) || (ms.member != member)
                                }
                              )

                            case "GT" =>
                              values.filter(ms =>
                                scoreMap.exists { case (member, score) =>
                                  (member == ms.member && score < ms.score) || (ms.member != member)
                                }
                              )
                          }.getOrElse(values)

            newScoreMap =
              if (incrOption.isDefined) {
                val ms = values.head
                scoreMap + (ms.member -> (scoreMap.getOrElse(ms.member, 0d) + ms.score))
              } else
                scoreMap ++ valuesToAdd.map(ms => ms.member -> ms.score)

            incrScore = incrOption.map { _ =>
                          val ms = values.head
                          scoreMap.getOrElse(ms.member, 0d) + ms.score
                        }

            valuesChanged = changedOption.map(_ => valuesToAdd.size).getOrElse(newScoreMap.size - scoreMap.size)
            _            <- sortedSets.put(key, newScoreMap)
          } yield incrScore.fold[RespValue](RespValue.Integer(valuesChanged.toLong))(result =>
            RespValue.bulkString(result.toString)
          )
        )

      case api.SortedSets.ZCard =>
        val key = input(0).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
          } yield RespValue.Integer(scoreMap.size.toLong)
        )

      case api.SortedSets.ZCount =>
        val key = input(0).asString
        val min = input(1).asLong
        val max = input(2).asLong

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            result    = scoreMap.filter { case (_, score) => score >= min && score <= max }
          } yield RespValue.Integer(result.size.toLong)
        )

      case api.SortedSets.ZDiff =>
        val numkeys          = input(0).asLong
        val keys             = input.drop(1).take(numkeys.toInt).map(_.asString)
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        orWrongType(forAll(keys)(isSortedSet))(
          for {
            sourceMaps <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))
            diffMap     = sourceMaps.reduce[Map[String, Double]] { case (a, b) => (a -- b.keySet) ++ (b -- a.keySet) }
            result =
              if (withScoresOption.isDefined)
                Chunk.fromIterable(diffMap.toArray.flatMap { case (v, s) =>
                  Chunk(bulkString(v), bulkString(s.toString))
                })
              else
                Chunk.fromIterable(diffMap.keys.map(bulkString))
          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZDiffStore =>
        val destination = input(0).asString
        val numkeys     = input(1).asLong
        val keys        = input.drop(2).take(numkeys.toInt).map(_.asString)

        orWrongType(forAll(keys :+ destination)(isSortedSet))(
          for {
            sourceMaps <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))
            diffMap     = sourceMaps.reduce[Map[String, Double]] { case (a, b) => (a -- b.keySet) ++ (b -- a.keySet) }
            _          <- sortedSets.put(destination, diffMap)
          } yield RespValue.Integer(diffMap.size.toLong)
        )

      case api.SortedSets.ZIncrBy =>
        val key       = input(0).asString
        val increment = input(1).asLong
        val member    = input(2).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            resultScore = scoreMap
                            .get(member)
                            .map(score => score + increment)
                            .getOrElse(increment.toDouble)

            resultSet = scoreMap + (member -> resultScore)
            _        <- sortedSets.put(key, resultSet)
          } yield RespValue.bulkString(resultScore.toString)
        )

      case api.SortedSets.ZInter =>
        val numKeys          = input(0).asLong
        val keys             = input.drop(1).take(numKeys.toInt).map(_.asString)
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        val options = input.map(_.asString).zipWithIndex
        val aggregate =
          options
            .find(_._1 == "AGGREGATE")
            .map(_._2)
            .map(idx => input(idx + 1).asString)
            .map {
              case "SUM" => (_: Double) + (_: Double)
              case "MIN" => Math.min(_: Double, _: Double)
              case "MAX" => Math.max(_: Double, _: Double)
            }
            .getOrElse((_: Double) + (_: Double))

        val weights =
          options
            .find(_._1 == "WEIGHTS")
            .map(_._2)
            .map(idx =>
              input
                .drop(idx + 1)
                .takeWhile(v => Try(v.asString.stripSuffix(".0").toLong).isSuccess)
                .map(_.asString.stripSuffix(".0").toLong)
            )
            .getOrElse(Chunk.empty)

        orInvalidParameter(STM.succeed(!(weights.nonEmpty && weights.size != numKeys)))(
          orWrongType(forAll(keys)(isSortedSet))(
            for {
              sourceSets <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))

              intersectionKeys =
                sourceSets.map(_.keySet).reduce(_.intersect(_))

              weightedSets =
                sourceSets
                  .map(m => m.filter(m => intersectionKeys.contains(m._1)))
                  .zipAll(weights, Map.empty, 1L)
                  .map { case (scoreMap, weight) => scoreMap.map { case (member, score) => member -> score * weight } }

              intersectionMap =
                weightedSets
                  .flatMap(Chunk.fromIterable)
                  .groupBy(_._1)
                  .map { case (member, scores) => member -> scores.map(_._2).reduce(aggregate) }

              result =
                if (withScoresOption.isDefined)
                  Chunk.fromIterable(intersectionMap.toArray.sortBy(_._2).flatMap { case (v, s) =>
                    Chunk(bulkString(v), bulkString(s.toString))
                  })
                else
                  Chunk.fromIterable(intersectionMap.toArray.sortBy(_._2).map(e => bulkString(e._1)))

            } yield RespValue.Array(result)
          )
        )

      case api.SortedSets.ZInterStore =>
        val destination = input(0).asString
        val numKeys     = input(1).asLong.toInt
        val keys        = input.drop(2).take(numKeys).map(_.asString)

        val options = input.map(_.asString).zipWithIndex
        val aggregate =
          options
            .find(_._1 == "AGGREGATE")
            .map(_._2)
            .map(idx => input(idx + 1).asString)
            .map {
              case "SUM" => (_: Double) + (_: Double)
              case "MIN" => Math.min(_: Double, _: Double)
              case "MAX" => Math.max(_: Double, _: Double)
            }
            .getOrElse((_: Double) + (_: Double))

        val weights =
          options
            .find(_._1 == "WEIGHTS")
            .map(_._2)
            .map(idx =>
              input
                .drop(idx + 1)
                .takeWhile(v => Try(v.asString.stripSuffix(".0").toLong).isSuccess)
                .map(_.asString.stripSuffix(".0").toLong)
            )
            .getOrElse(Chunk.empty)

        orInvalidParameter(STM.succeed(!(weights.nonEmpty && weights.size != numKeys)))(
          orWrongType(forAll(keys :+ destination)(isSortedSet))(
            for {
              sourceSets <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))

              intersectionKeys =
                sourceSets.map(_.keySet).reduce(_.intersect(_))

              weightedSets =
                sourceSets
                  .map(m => m.filter(m => intersectionKeys.contains(m._1)))
                  .zipAll(weights, Map.empty, 1L)
                  .map { case (scoreMap, weight) => scoreMap.map { case (member, score) => member -> score * weight } }

              destinationResult =
                weightedSets
                  .flatMap(Chunk.fromIterable)
                  .groupBy(_._1)
                  .map { case (member, scores) => member -> scores.map(_._2).reduce(aggregate) }

              _ <- sortedSets.put(destination, destinationResult)
            } yield RespValue.Integer(destinationResult.size.toLong)
          )
        )

      case api.SortedSets.ZLexCount =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-"                    => LexMinimum.Unbounded
          case s if s.startsWith("(") => LexMinimum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMinimum.Closed(s.drop(1))
        }

        val max = input(2).asString match {
          case "+"                    => LexMaximum.Unbounded
          case s if s.startsWith("(") => LexMaximum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMaximum.Closed(s.drop(1))
        }

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            lexKeys   = scoreMap.keys.toArray.sorted

            minPredicate = (s: String) =>
                             min match {
                               case LexMinimum.Unbounded   => true
                               case LexMinimum.Open(key)   => s > key
                               case LexMinimum.Closed(key) => s >= key
                             }

            maxPredicate = (s: String) =>
                             max match {
                               case LexMaximum.Unbounded   => true
                               case LexMaximum.Open(key)   => s < key
                               case LexMaximum.Closed(key) => s <= key
                             }

            filtered = lexKeys.filter(s => minPredicate(s) && maxPredicate(s))

            result = Chunk.fromIterable(filtered.map(bulkString))
          } yield RespValue.Integer(result.size.toLong)
        )

      case api.SortedSets.ZRangeByLex =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-"                    => LexMinimum.Unbounded
          case s if s.startsWith("(") => LexMinimum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMinimum.Closed(s.drop(1))
        }

        val max = input(2).asString match {
          case "+"                    => LexMaximum.Unbounded
          case s if s.startsWith("(") => LexMaximum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMaximum.Closed(s.drop(1))
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                            .map(_._1)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.keys.toArray.sorted)

            minPredicate = (s: String) =>
                             min match {
                               case LexMinimum.Unbounded   => true
                               case LexMinimum.Open(key)   => s > key
                               case LexMinimum.Closed(key) => s >= key
                             }

            maxPredicate = (s: String) =>
                             max match {
                               case LexMaximum.Unbounded   => true
                               case LexMaximum.Open(key)   => s < key
                               case LexMaximum.Closed(key) => s <= key
                             }

            filtered = lexKeys.filter(s => minPredicate(s) && maxPredicate(s))

            bounds = (min, max) match {
                       case (LexMinimum.Unbounded, LexMaximum.Unbounded) => filtered
                       case (LexMinimum.Unbounded, _)                    => filtered.dropRight(1)
                       case (_, LexMaximum.Unbounded)                    => filtered.drop(1)
                       case (_, _)                                       => filtered.drop(1).dropRight(1)
                     }

            result = Chunk.fromIterable(bounds.map(bulkString))
          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZRevRangeByLex =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-"                    => LexMinimum.Unbounded
          case s if s.startsWith("(") => LexMinimum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMinimum.Closed(s.drop(1))
        }

        val max = input(2).asString match {
          case "+"                    => LexMaximum.Unbounded
          case s if s.startsWith("(") => LexMaximum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMaximum.Closed(s.drop(1))
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._1)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                            .map(_._1)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.keys.toArray.sorted).reverse

            minPredicate = (s: String) =>
                             min match {
                               case LexMinimum.Unbounded   => true
                               case LexMinimum.Open(key)   => s < key
                               case LexMinimum.Closed(key) => s <= key
                             }

            maxPredicate = (s: String) =>
                             max match {
                               case LexMaximum.Unbounded   => true
                               case LexMaximum.Open(key)   => s > key
                               case LexMaximum.Closed(key) => s >= key
                             }

            filtered = lexKeys.filter(s => minPredicate(s) && maxPredicate(s))

            result = Chunk.fromIterable(filtered.map(bulkString))
          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZRemRangeByLex =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-"                    => LexMinimum.Unbounded
          case s if s.startsWith("(") => LexMinimum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMinimum.Closed(s.drop(1))
        }

        val max = input(2).asString match {
          case "+"                    => LexMaximum.Unbounded
          case s if s.startsWith("(") => LexMaximum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMaximum.Closed(s.drop(1))
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                            .map(_._1)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.keys.toArray.sorted)

            minPredicate = (s: String) =>
                             min match {
                               case LexMinimum.Unbounded   => true
                               case LexMinimum.Open(key)   => s > key
                               case LexMinimum.Closed(key) => s >= key
                             }

            maxPredicate = (s: String) =>
                             max match {
                               case LexMaximum.Unbounded   => true
                               case LexMaximum.Open(key)   => s < key
                               case LexMaximum.Closed(key) => s <= key
                             }

            filtered = lexKeys.filter(s => minPredicate(s) && maxPredicate(s))

            _ <- sortedSets.put(key, scoreMap -- filtered)
          } yield RespValue.Integer(filtered.length.toLong)
        )

      case api.SortedSets.ZRemRangeByRank =>
        val key   = input(0).asString
        val start = input(1).asLong.toInt
        val stop  = input(2).asLong.toInt

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            rank      = scoreMap.toArray.sortBy(_._2)
            result    = rank.slice(start, if (stop < 0) rank.length + stop else stop + 1)
            _        <- sortedSets.put(key, scoreMap -- result.map(_._1))
          } yield RespValue.Integer(result.length.toLong)
        )

      case api.SortedSets.ZRangeByScore =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-inf"                 => ScoreMinimum.Infinity
          case s if s.startsWith("(") => ScoreMinimum.Open(s.drop(1).toDouble)
          case s                      => ScoreMinimum.Closed(s.toDouble)
        }

        val max = input(2).asString match {
          case "+inf"                 => ScoreMaximum.Infinity
          case s if s.startsWith("(") => ScoreMaximum.Open(s.drop(1).toDouble)
          case s                      => ScoreMaximum.Closed(s.toDouble)
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        val withScoresOption = input.map(_.asString).indexOf("WITHSCORES") match {
          case -1 => false
          case _  => true
        }

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.toArray.sortBy(_._2))

            minPredicate = (s: Double) =>
                             min match {
                               case _: ScoreMinimum.Infinity.type => true
                               case ScoreMinimum.Open(key)        => s > key
                               case ScoreMinimum.Closed(key)      => s >= key
                             }

            maxPredicate = (s: Double) =>
                             max match {
                               case _: ScoreMaximum.Infinity.type => true
                               case ScoreMaximum.Open(key)        => s < key
                               case ScoreMaximum.Closed(key)      => s <= key
                             }

            filtered = lexKeys.filter { case (_, s) => minPredicate(s) && maxPredicate(s) }

            result =
              if (withScoresOption)
                Chunk.fromIterable(filtered.flatMap { case (k, s) => bulkString(k) :: bulkString(s.toString) :: Nil })
              else
                Chunk.fromIterable(filtered.map { case (k, _) => bulkString(k) })

          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZRevRangeByScore =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-inf"                 => ScoreMinimum.Infinity
          case s if s.startsWith("(") => ScoreMinimum.Open(s.drop(1).toDouble)
          case s                      => ScoreMinimum.Closed(s.toDouble)
        }

        val max = input(2).asString match {
          case "+inf"                 => ScoreMaximum.Infinity
          case s if s.startsWith("(") => ScoreMaximum.Open(s.drop(1).toDouble)
          case s                      => ScoreMaximum.Closed(s.toDouble)
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        val withScoresOption = input.map(_.asString).indexOf("WITHSCORES") match {
          case -1 => false
          case _  => true
        }

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption.map(_ + 1)
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .reverse
                            .slice(offset.toInt, offset.toInt + count.toInt)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.toArray.sortBy(_._2).reverse)

            minPredicate = (s: Double) =>
                             min match {
                               case _: ScoreMinimum.Infinity.type => true
                               case ScoreMinimum.Open(key)        => s < key
                               case ScoreMinimum.Closed(key)      => s <= key
                             }

            maxPredicate = (s: Double) =>
                             max match {
                               case _: ScoreMaximum.Infinity.type => true
                               case ScoreMaximum.Open(key)        => s > key
                               case ScoreMaximum.Closed(key)      => s >= key
                             }

            filtered = lexKeys.filter { case (_, s) => minPredicate(s) && maxPredicate(s) }

            result =
              if (withScoresOption)
                Chunk.fromIterable(filtered.flatMap { case (k, s) => bulkString(k) :: bulkString(s.toString) :: Nil })
              else
                Chunk.fromIterable(filtered.map { case (k, _) => bulkString(k) })

          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZRemRangeByScore =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-inf"                 => ScoreMinimum.Infinity
          case s if s.startsWith("(") => ScoreMinimum.Open(s.drop(1).toDouble)
          case s if s.startsWith("[") => ScoreMinimum.Closed(s.drop(1).toDouble)
        }

        val max = input(2).asString match {
          case "+inf"                 => ScoreMaximum.Infinity
          case s if s.startsWith("(") => ScoreMaximum.Open(s.drop(1).toDouble)
          case s if s.startsWith("[") => ScoreMaximum.Closed(s.drop(1).toDouble)
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.toArray.sortBy(_._2))

            minPredicate = (s: Double) =>
                             min match {
                               case _: ScoreMinimum.Infinity.type => true
                               case ScoreMinimum.Open(key)        => s > key
                               case ScoreMinimum.Closed(key)      => s >= key
                             }

            maxPredicate = (s: Double) =>
                             max match {
                               case _: ScoreMaximum.Infinity.type => true
                               case ScoreMaximum.Open(key)        => s < key
                               case ScoreMaximum.Closed(key)      => s <= key
                             }

            filtered = lexKeys.filter { case (_, s) => minPredicate(s) && maxPredicate(s) }

            _ <- sortedSets.put(key, scoreMap -- filtered.map(_._1))
          } yield RespValue.Integer(filtered.length.toLong)
        )

      case api.SortedSets.ZPopMin =>
        val key   = input(0).asString
        val count = input.drop(1).headOption.map(_.asString.toInt).getOrElse(1)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            results   = scoreMap.toArray.sortBy { case (_, score) => score }.take(count)
            _        <- sortedSets.put(key, scoreMap -- results.map(_._1))
          } yield RespValue.Array(
            Chunk
              .fromIterable(results)
              .flatMap(ms => Chunk(RespValue.bulkString(ms._1), RespValue.bulkString(ms._2.toString.stripSuffix(".0"))))
          )
        )

      case api.SortedSets.ZPopMax =>
        val key   = input(0).asString
        val count = input.drop(1).headOption.map(_.asString.toInt).getOrElse(1)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            results   = scoreMap.toArray.sortBy { case (_, score) => score }.reverse.take(count)
            _        <- sortedSets.put(key, scoreMap -- results.map(_._1))
          } yield RespValue.Array(
            Chunk
              .fromIterable(results)
              .flatMap(ms => Chunk(RespValue.bulkString(ms._1), RespValue.bulkString(ms._2.toString.stripSuffix(".0"))))
          )
        )

      case api.SortedSets.ZRank =>
        val key    = input(0).asString
        val member = input(1).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            rank = scoreMap.toArray.sortBy(_._2).map(_._1).indexOf(member) match {
                     case -1  => None
                     case idx => Some(idx)
                   }
          } yield rank.fold[RespValue](RespValue.NullBulkString)(result => RespValue.Integer(result.toLong))
        )

      case api.SortedSets.ZRem =>
        val key     = input(0).asString
        val members = input.tail.map(_.asString)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            newSet    = scoreMap.filterNot { case (v, _) => members.contains(v) }
            _        <- sortedSets.put(key, newSet)
          } yield RespValue.Integer(scoreMap.size.toLong - newSet.size.toLong)
        )

      case api.SortedSets.ZRevRank =>
        val key    = input(0).asString
        val member = input(1).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            rank = scoreMap.toArray.sortBy(_._2).reverse.map(_._1).indexOf(member) match {
                     case -1  => None
                     case idx => Some(idx)
                   }
          } yield rank.fold[RespValue](RespValue.NullBulkString)(result => RespValue.Integer(result.toLong))
        )

      case api.SortedSets.ZScore =>
        val key    = input(0).asString
        val member = input(1).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap  <- sortedSets.getOrElse(key, Map.empty)
            maybeScore = scoreMap.get(member)
          } yield maybeScore.fold[RespValue](RespValue.NullBulkString)(result => RespValue.bulkString(result.toString))
        )

      case api.SortedSets.Zmscore =>
        val key     = input(0).asString
        val members = input.tail.map(_.asString)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap   <- sortedSets.getOrElse(key, Map.empty)
            maybeScores = members.map(m => scoreMap.get(m))
            result = maybeScores.map {
                       case Some(v) => RespValue.bulkString(v.toString)
                       case None    => RespValue.NullBulkString
                     }
          } yield RespValue.array(result: _*)
        )

      case api.SortedSets.ZRange =>
        val key              = input.head.asString
        val start            = input(1).asString.toInt
        val end              = input(2).asString.toInt
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            slice =
              if (end < 0)
                scoreMap.toArray.sortBy(_._2).slice(start, scoreMap.size + 1 + end)
              else
                scoreMap.toArray.sortBy(_._2).slice(start, end + 1)

            result = withScoresOption.fold(slice.map(_._1))(_ =>
                       slice.flatMap { case (v, s) => Array(v, s.toString.stripSuffix(".0")) }
                     )
          } yield RespValue.Array(Chunk.fromIterable(result) map RespValue.bulkString)
        )

      case api.SortedSets.ZRevRange =>
        val key              = input.head.asString
        val start            = input(1).asString.toInt
        val end              = input(2).asString.toInt
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            slice =
              if (end < 0)
                scoreMap.toArray.sortBy(_._2).reverse.slice(start, scoreMap.size + 1 + end)
              else
                scoreMap.toArray.sortBy(_._2).reverse.slice(start, end + 1)

            result = withScoresOption.fold(slice.map(_._1))(_ =>
                       slice.flatMap { case (v, s) => Array(v, s.toString.stripSuffix(".0")) }
                     )
          } yield RespValue.Array(Chunk.fromIterable(result) map RespValue.bulkString)
        )

      case api.SortedSets.ZScan =>
        val key   = input.head.asString
        val start = input(1).asString.toInt

        val maybeRegex =
          if (input.size > 2) input(2).asString match {
            case "MATCH" => Some(input(3).asString.replace("*", ".*").r)
            case _       => None
          }
          else None

        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val maybeCount =
          if (input.size > 4) maybeGetCount(input(4), input(5))
          else if (input.size > 2) maybeGetCount(input(2), input(3))
          else None

        val end = start + maybeCount.getOrElse(10)

        orWrongType(isSortedSet(key))(
          {
            for {
              scoreMap <- sortedSets.getOrElse(key, Map.empty)
              filtered =
                maybeRegex.map(regex => scoreMap.filter(s => regex.pattern.matcher(s._1).matches)).getOrElse(scoreMap)
              resultSet = filtered.toArray.sortBy(_._2).slice(start, end)
              nextIndex = if (filtered.size <= end) 0 else end
              expand    = resultSet.flatMap(v => Array(v._1, v._2.toString.stripSuffix(".0")))
              results   = Replies.array(expand)
            } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
          }
        )

      case api.SortedSets.ZUnionStore =>
        val destination = input(0).asString
        val numKeys     = input(1).asLong.toInt
        val keys        = input.drop(2).take(numKeys).map(_.asString)

        val options = input.map(_.asString).zipWithIndex
        val aggregate =
          options
            .find(_._1 == "AGGREGATE")
            .map(_._2)
            .map(idx => input(idx + 1).asString)
            .map {
              case "SUM" => (_: Double) + (_: Double)
              case "MIN" => Math.min(_: Double, _: Double)
              case "MAX" => Math.max(_: Double, _: Double)
            }
            .getOrElse((_: Double) + (_: Double))

        val weights =
          options
            .find(_._1 == "WEIGHTS")
            .map(_._2)
            .map(idx =>
              input
                .drop(idx + 1)
                .takeWhile(v => Try(v.asString.stripSuffix(".0").toLong).isSuccess)
                .map(_.asString.stripSuffix(".0").toLong)
            )
            .getOrElse(Chunk.empty)

        orInvalidParameter(STM.succeed(!(weights.nonEmpty && weights.size != numKeys)))(
          orWrongType(forAll(keys :+ destination)(isSortedSet))(
            for {
              sourceSets <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))

              unionKeys =
                sourceSets.map(_.keySet).reduce(_.union(_))

              weightedSets =
                sourceSets
                  .map(m => m.filter(m => unionKeys.contains(m._1)))
                  .zipAll(weights, Map.empty, 1L)
                  .map { case (scoreMap, weight) => scoreMap.map { case (member, score) => member -> score * weight } }

              destinationResult =
                weightedSets
                  .flatMap(Chunk.fromIterable)
                  .groupBy(_._1)
                  .map { case (member, scores) => member -> scores.map(_._2).reduce(aggregate) }

              _ <- sortedSets.put(destination, destinationResult)
            } yield RespValue.Integer(destinationResult.size.toLong)
          )
        )

      case api.SortedSets.ZUnion =>
        val numKeys          = input(0).asLong.toInt
        val keys             = input.drop(1).take(numKeys).map(_.asString)
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        val options = input.map(_.asString).zipWithIndex
        val aggregate =
          options
            .find(_._1 == "AGGREGATE")
            .map(_._2)
            .map(idx => input(idx + 1).asString)
            .map {
              case "SUM" => (_: Double) + (_: Double)
              case "MIN" => Math.min(_: Double, _: Double)
              case "MAX" => Math.max(_: Double, _: Double)
            }
            .getOrElse((_: Double) + (_: Double))

        val weights =
          options
            .find(_._1 == "WEIGHTS")
            .map(_._2)
            .map(idx =>
              input
                .drop(idx + 1)
                .takeWhile(v => Try(v.asString.stripSuffix(".0").toLong).isSuccess)
                .map(_.asString.stripSuffix(".0").toLong)
            )
            .getOrElse(Chunk.empty)

        orInvalidParameter(STM.succeed(!(weights.nonEmpty && weights.size != numKeys)))(
          orWrongType(forAll(keys)(isSortedSet))(
            for {
              sourceSets <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))

              unionKeys =
                sourceSets.map(_.keySet).reduce(_.union(_))

              weightedSets =
                sourceSets
                  .map(m => m.filter(m => unionKeys.contains(m._1)))
                  .zipAll(weights, Map.empty, 1L)
                  .map { case (scoreMap, weight) => scoreMap.map { case (member, score) => member -> score * weight } }

              unionMap =
                weightedSets
                  .flatMap(Chunk.fromIterable)
                  .groupBy(_._1)
                  .map { case (member, scores) => member -> scores.map(_._2).reduce(aggregate) }

              result =
                if (withScoresOption.isDefined)
                  Chunk.fromIterable(unionMap.toArray.sortBy(_._2).flatMap { case (v, s) =>
                    Chunk(bulkString(v), bulkString(s.toString))
                  })
                else
                  Chunk.fromIterable(unionMap.toArray.sortBy(_._2).map(e => bulkString(e._1)))

            } yield RespValue.Array(result)
          )
        )

      case api.SortedSets.ZRandMember =>
        val key              = input(0).asString
        val maybeCount       = input.tail.headOption.map(b => b.asString.toLong)
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            asVector  = scoreMap.toVector
            res <- maybeCount match {
                     case None =>
                       selectOne(asVector, randomPick).map {
                         _.fold(RespValue.NullBulkString: RespValue)(s => RespValue.bulkString(s._1))
                       }

                     case Some(n) if n > 0 =>
                       selectN(asVector, n, randomPick).map[RespValue] { values =>
                         if (withScoresOption.isDefined) {
                           val flatMemberScores = values.flatMap { case (m, s) => m :: s.toString :: Nil }
                           if (flatMemberScores.isEmpty)
                             RespValue.NullArray
                           else
                             Replies.array(flatMemberScores)
                         } else {
                           if (values.isEmpty)
                             RespValue.NullArray
                           else
                             Replies.array(values.map(_._1))
                         }
                       }

                     case Some(n) if n < 0 =>
                       selectNWithReplacement(asVector, -n, randomPick).map[RespValue] { values =>
                         if (withScoresOption.isDefined) {
                           val flatMemeberScore = values.flatMap { case (m, s) => m :: s.toString :: Nil }
                           if (flatMemeberScore.isEmpty)
                             RespValue.NullArray
                           else
                             Replies.array(flatMemeberScore)
                         } else {
                           if (values.isEmpty)
                             RespValue.NullArray
                           else
                             Replies.array(values.map(_._1))
                         }
                       }

                     case Some(_) => STM.succeedNow(RespValue.NullBulkString)
                   }
          } yield res
        )

      case _ => STM.succeedNow(RespValue.Error("ERR unknown command"))
    }
  }

  private[this] def orWrongType(predicate: USTM[Boolean])(
    program: => USTM[RespValue]
  ): USTM[RespValue] =
    STM.ifM(predicate)(program, STM.succeedNow(Replies.WrongType))

  private[this] def orInvalidParameter(predicate: USTM[Boolean])(
    program: => USTM[RespValue]
  ): USTM[RespValue] =
    STM.ifM(predicate)(program, STM.succeedNow(Replies.Error))

  // check whether the key is a set or unused.
  private[this] def isSet(name: String): STM[Nothing, Boolean] =
    for {
      isString    <- strings.contains(name)
      isList      <- lists.contains(name)
      isHyper     <- hyperLogLogs.contains(name)
      isHash      <- hashes.contains(name)
      isSortedSet <- sortedSets.contains(name)
    } yield !isString && !isList && !isHyper && !isHash && !isSortedSet

  // check whether the key is a list or unused.
  private[this] def isList(name: String): STM[Nothing, Boolean] =
    for {
      isString <- strings.contains(name)
      isSet    <- sets.contains(name)
      isHyper  <- hyperLogLogs.contains(name)
      isHash   <- hashes.contains(name)
    } yield !isString && !isSet && !isHyper && !isHash

  //check whether the key is a hyperLogLog or unused.
  private[this] def isHyperLogLog(name: String): ZSTM[Any, Nothing, Boolean] =
    for {
      isString    <- strings.contains(name)
      isSet       <- sets.contains(name)
      isList      <- lists.contains(name)
      isHash      <- hashes.contains(name)
      isSortedSet <- sortedSets.contains(name)
    } yield !isString && !isSet && !isList && !isHash && !isSortedSet

  //check whether the key is a hash or unused.
  private[this] def isHash(name: String): ZSTM[Any, Nothing, Boolean] =
    for {
      isString    <- strings.contains(name)
      isSet       <- sets.contains(name)
      isList      <- lists.contains(name)
      isHyper     <- hyperLogLogs.contains(name)
      isSortedSet <- sortedSets.contains(name)
    } yield !isString && !isSet && !isList && !isHyper && !isSortedSet

  //check whether the key is a hash or unused.
  private[this] def isSortedSet(name: String): ZSTM[Any, Nothing, Boolean] =
    for {
      isString <- strings.contains(name)
      isSet    <- sets.contains(name)
      isList   <- lists.contains(name)
      isHyper  <- hyperLogLogs.contains(name)
      isHash   <- hashes.contains(name)
    } yield !isString && !isSet && !isList && !isHyper && !isHash

  @tailrec
  private[this] def dropWhileLimit[A](xs: Chunk[A])(p: A => Boolean, k: Int): Chunk[A] =
    if (k <= 0 || xs.isEmpty || !p(xs.head)) xs
    else dropWhileLimit(xs.tail)(p, k - 1)

  private[this] def selectN[A](values: Vector[A], n: Long, pickRandom: Int => USTM[Int]): USTM[List[A]] = {
    def go(remaining: Vector[A], toPick: Long, acc: List[A]): USTM[List[A]] =
      (remaining, toPick) match {
        case (Vector(), _) | (_, 0) => STM.succeed(acc)
        case _ =>
          pickRandom(remaining.size).flatMap { index =>
            val x  = remaining(index)
            val xs = remaining.patch(index, Nil, 1)
            go(xs, toPick - 1, x :: acc)
          }
      }

    go(values, Math.max(n, 0), Nil)
  }

  private[this] def selectOne[A](values: Vector[A], pickRandom: Int => USTM[Int]): USTM[Option[A]] =
    if (values.isEmpty) STM.succeedNow(None)
    else pickRandom(values.size).map(index => Some(values(index)))

  private[this] def selectNWithReplacement[A](
    values: Vector[A],
    n: Long,
    pickRandom: Int => USTM[Int]
  ): USTM[List[A]] =
    STM
      .loop(Math.max(n, 0))(_ > 0, _ - 1)(_ => selectOne(values, pickRandom))
      .map(_.flatten)

  private[this] def sInter(mainKey: String, otherKeys: Chunk[String]): STM[Unit, Set[String]] = {
    sealed trait State
    object State {
      case object WrongType extends State

      final case class Continue(values: Set[String]) extends State
    }

    def get(key: String): STM[Nothing, State] =
      STM.ifM(isSet(key))(
        sets.get(key).map(_.fold[State](State.Continue(Set.empty))(State.Continue)),
        STM.succeedNow(State.WrongType)
      )

    def step(state: State, next: String): STM[Nothing, State] =
      state match {
        case State.WrongType => STM.succeedNow(State.WrongType)
        case State.Continue(values) =>
          get(next).map {
            case State.Continue(otherValues) =>
              val intersection = values.intersect(otherValues)
              State.Continue(intersection)
            case s => s
          }
      }

    for {
      init  <- get(mainKey)
      state <- STM.foldLeft(otherKeys)(init)(step)
      result <- state match {
                  case State.WrongType        => STM.fail(())
                  case State.Continue(values) => STM.succeedNow(values)
                }
    } yield result
  }

  private[this] def forAll[A](chunk: Chunk[A])(f: A => STM[Nothing, Boolean]) =
    STM.foldLeft(chunk)(true) { case (b, a) => f(a).map(b && _) }

  private[this] object Replies {
    val Ok: RespValue.SimpleString = RespValue.SimpleString("OK")
    val WrongType: RespValue.Error = RespValue.Error("WRONGTYPE")
    val Error: RespValue.Error     = RespValue.Error("ERR")

    def array(values: Iterable[String]): RespValue.Array =
      RespValue.array(values.map(RespValue.bulkString).toList: _*)

    val EmptyArray: RespValue.Array = RespValue.array()
  }

}

private[redis] object TestExecutor {
  lazy val live: URLayer[zio.random.Random, RedisExecutor] = {
    val executor = for {
      seed         <- random.nextInt
      sRandom       = new scala.util.Random(seed)
      ref          <- TRef.make(LazyList.continually((i: Int) => sRandom.nextInt(i))).commit
      randomPick    = (i: Int) => ref.modify(s => (s.head(i), s.tail))
      sets         <- TMap.empty[String, Set[String]].commit
      strings      <- TMap.empty[String, String].commit
      hyperLogLogs <- TMap.empty[String, Set[String]].commit
      lists        <- TMap.empty[String, Chunk[String]].commit
      hashes       <- TMap.empty[String, Map[String, String]].commit
      sortedSets   <- TMap.empty[String, Map[String, Double]].commit
    } yield new TestExecutor(lists, sets, strings, randomPick, hyperLogLogs, hashes, sortedSets)

    executor.toLayer
  }

}
