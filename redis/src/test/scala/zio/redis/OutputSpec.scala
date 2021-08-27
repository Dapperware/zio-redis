package zio.redis

import java.net.InetAddress

import zio.duration._
import zio.redis.Output._
import zio.redis.RedisError.{ ProtocolError, _ }
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Task }

object OutputSpec extends BaseSpec {

  def spec: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("Output decoders")(
      suite("errors")(
        testM("protocol errors") {
          for {
            res <- Task(BoolOutput.unsafeDecode(RespValue.Error(s"ERR $Noise"))).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(Noise))))
        },
        testM("wrong type") {
          for {
            res <- Task(BoolOutput.unsafeDecode(RespValue.Error(s"WRONGTYPE $Noise"))).either
          } yield assert(res)(isLeft(equalTo(WrongType(Noise))))
        }
      ),
      suite("boolean")(
        testM("extract true") {
          for {
            res <- Task(BoolOutput.unsafeDecode(RespValue.Integer(1L)))
          } yield assert(res)(isTrue)
        },
        testM("extract false") {
          for {
            res <- Task(BoolOutput.unsafeDecode(RespValue.Integer(0)))
          } yield assert(res)(isFalse)
        }
      ),
      suite("chunk")(
        testM("extract empty arrays") {
          for {
            res <- Task(ChunkOutput(MultiStringOutput).unsafeDecode(RespValue.Array(Chunk.empty)))
          } yield assert(res)(isEmpty)
        },
        testM("extract non-empty arrays") {
          for {
            res <-
              Task(
                ChunkOutput(MultiStringOutput).unsafeDecode(
                  RespValue.array(RespValue.bulkString("foo"), RespValue.bulkString("bar"))
                )
              )
          } yield assert(res)(hasSameElements(Chunk("foo", "bar")))
        }
      ),
      suite("double")(
        testM("extract numbers") {
          val num = 42.3
          for {
            res <- Task(DoubleOutput.unsafeDecode(RespValue.bulkString(num.toString)))
          } yield assert(res)(equalTo(num))
        },
        testM("report number format exceptions as protocol errors") {
          val bad = "ok"
          for {
            res <- Task(DoubleOutput.unsafeDecode(RespValue.bulkString(bad))).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"'$bad' isn't a double."))))
        }
      ),
      suite("durations")(
        suite("milliseconds")(
          testM("extract milliseconds") {
            val num = 42L
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(num)))
            } yield assert(res)(equalTo(num.millis))
          },
          testM("report error for non-existing key") {
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(-2L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          testM("report error for key without TTL") {
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(-1L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          }
        ),
        suite("seconds")(
          testM("extract seconds") {
            val num = 42L
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(RespValue.Integer(num)))
            } yield assert(res)(equalTo(num.seconds))
          },
          testM("report error for non-existing key") {
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(RespValue.Integer(-2L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          testM("report error for key without TTL") {
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(RespValue.Integer(-1L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          }
        )
      ),
      suite("long")(
        testM("extract positive numbers") {
          val num = 42L
          for {
            res <- Task(LongOutput.unsafeDecode(RespValue.Integer(num)))
          } yield assert(res)(equalTo(num))
        },
        testM("extract negative numbers") {
          val num = -42L
          for {
            res <- Task(LongOutput.unsafeDecode(RespValue.Integer(num)))
          } yield assert(res)(equalTo(num))
        }
      ),
      suite("chunkLong")(
        testM("extract empty array") {
          for {
            res <- Task(ChunkOutput(LongOutput).unsafeDecode(RespValue.NullArray))
          } yield assert(res)(equalTo(Chunk.empty))
        },
        testM("extract array of long values") {
          val respArray = RespValue.array(RespValue.Integer(1L), RespValue.Integer(2L), RespValue.Integer(3L))
          for {
            res <- Task(ChunkOutput(LongOutput).unsafeDecode(respArray))
          } yield assert(res)(equalTo(Chunk(1L, 2L, 3L)))
        },
        testM("fail when one value is not a long") {
          val respArray =
            RespValue.array(RespValue.Integer(1L), RespValue.bulkString("not a long"), RespValue.Integer(3L))
          for {
            res <- Task(ChunkOutput(LongOutput).unsafeDecode(respArray)).run
          } yield assert(res)(fails(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("optional")(
        testM("extract None") {
          for {
            res <- Task(OptionalOutput(UnitOutput).unsafeDecode(RespValue.NullBulkString))
          } yield assert(res)(isNone)
        },
        testM("extract some") {
          for {
            res <- Task(OptionalOutput(UnitOutput).unsafeDecode(RespValue.SimpleString("OK")))
          } yield assert(res)(isSome(isUnit))
        }
      ),
      suite("scan")(
        testM("extract cursor and elements") {
          val input = RespValue.array(
            RespValue.bulkString("5"),
            RespValue.array(RespValue.bulkString("foo"), RespValue.bulkString("bar"))
          )
          for {
            res <- Task(ScanOutput(MultiStringOutput).unsafeDecode(input))
          } yield assert(res)(equalTo(5L -> Chunk("foo", "bar")))
        }
      ),
      suite("string")(
        testM("extract strings") {
          for {
            res <- Task(MultiStringOutput.unsafeDecode(RespValue.bulkString(Noise)))
          } yield assert(res)(equalTo(Noise))
        }
      ),
      suite("unit")(
        testM("extract unit") {
          for {
            res <- Task(UnitOutput.unsafeDecode(RespValue.SimpleString("OK")))
          } yield assert(res)(isUnit)
        }
      ),
      suite("reset") {
        testM("extract unit") {
          for {
            res <- Task(ResetOutput.unsafeDecode(RespValue.SimpleString("RESET")))
          } yield assert(res)(isUnit)
        }
      },
      suite("keyElem")(
        testM("extract none") {
          for {
            res <- Task(KeyElemOutput.unsafeDecode(RespValue.NullArray))
          } yield assert(res)(isNone)
        },
        testM("extract key and element") {
          for {
            res <-
              Task(KeyElemOutput.unsafeDecode(RespValue.array(RespValue.bulkString("key"), RespValue.bulkString("a"))))
          } yield assert(res)(isSome(equalTo(("key", "a"))))
        },
        testM("report invalid input as protocol error") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- Task(KeyElemOutput.unsafeDecode(input)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("multiStringChunk")(
        testM("extract one empty value") {
          for {
            res <- Task(MultiStringChunkOutput(MultiStringOutput).unsafeDecode(RespValue.NullBulkString))
          } yield assert(res)(isEmpty)
        },
        testM("extract one multi-string value") {
          for {
            res <- Task(MultiStringChunkOutput(MultiStringOutput).unsafeDecode(RespValue.bulkString("ab")))
          } yield assert(res)(hasSameElements(Chunk("ab")))
        },
        testM("extract one array value") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- Task(MultiStringChunkOutput(MultiStringOutput).unsafeDecode(input))
          } yield assert(res)(hasSameElements(Chunk("1", "2", "3")))
        }
      ),
      suite("chunkOptionalMultiString")(
        testM("extract one empty value") {
          for {
            res <- Task(ChunkOutput(OptionalOutput(MultiStringOutput)).unsafeDecode(RespValue.NullArray))
          } yield assert(res)(isEmpty)
        },
        testM("extract array with one non-empty element") {
          for {
            res <-
              Task(
                ChunkOutput(OptionalOutput(MultiStringOutput)).unsafeDecode(RespValue.array(RespValue.bulkString("ab")))
              )
          } yield assert(res)(equalTo(Chunk(Some("ab"))))
        },
        testM("extract array with multiple non-empty elements") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- Task(ChunkOutput(OptionalOutput(MultiStringOutput)).unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some("1"), Some("2"), Some("3"))))
        },
        testM("extract array with empty and non-empty elements") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.NullBulkString, RespValue.bulkString("3"))
          for {
            res <- Task(ChunkOutput(OptionalOutput(MultiStringOutput)).unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some("1"), None, Some("3"))))
        }
      ),
      suite("chunkOptionalLong")(
        testM("extract one empty value") {
          for {
            res <- Task(ChunkOutput(OptionalOutput(LongOutput)).unsafeDecode(RespValue.Array(Chunk.empty)))
          } yield assert(res)(isEmpty)
        },
        testM("extract array with empty and non-empty elements") {
          val input = RespValue.array(
            RespValue.Integer(1L),
            RespValue.NullBulkString,
            RespValue.Integer(2L),
            RespValue.Integer(3L)
          )
          for {
            res <- Task(ChunkOutput(OptionalOutput(LongOutput)).unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some(1L), None, Some(2L), Some(3L))))
        },
        testM("extract array with non-empty elements") {
          val input = RespValue.array(
            RespValue.Integer(1L),
            RespValue.Integer(1L),
            RespValue.Integer(2L),
            RespValue.Integer(3L)
          )
          for {
            res <- Task(ChunkOutput(OptionalOutput(LongOutput)).unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some(1L), Some(1L), Some(2L), Some(3L))))
        }
      ),
      suite("stream")(
        testM("extract valid input") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("field"), RespValue.bulkString("value"))
            )
          )

          Task(StreamOutput[String, String, String]().unsafeDecode(input))
            .map(assert(_)(equalTo(Map("id" -> Map("field" -> "value")))))
        },
        testM("extract empty map") {
          Task(StreamOutput[String, String, String]().unsafeDecode(RespValue.array())).map(assert(_)(isEmpty))
        },
        testM("error when array of field-value pairs has odd length") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("field"), RespValue.bulkString("value")),
              RespValue.NullBulkString
            )
          )

          Task(StreamOutput[String, String, String]().unsafeDecode(input)).either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        testM("error when message has more then two elements") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("a"), RespValue.bulkString("b"), RespValue.bulkString("c"))
            )
          )

          Task(StreamOutput[String, String, String]().unsafeDecode(input)).either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("xPending")(
        testM("extract valid value") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          Task(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, Some("a"), Some("b"), Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        testM("extract when the smallest ID is null") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.NullBulkString,
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          Task(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, None, Some("b"), Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        testM("extract when the greatest ID is null") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.NullBulkString,
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          Task(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, Some("a"), None, Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        testM("extract when total number of pending messages is zero") {
          val input = RespValue.array(
            RespValue.Integer(0),
            RespValue.NullBulkString,
            RespValue.NullBulkString,
            RespValue.NullArray
          )
          Task(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(0L, None, None, Map.empty))))
        },
        testM("error when consumer array doesn't have two elements") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          Task(XPendingOutput.unsafeDecode(input)).either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("pendingMessages")(
        testM("extract valid value") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.bulkString("consumer"),
              RespValue.Integer(100),
              RespValue.Integer(10)
            ),
            RespValue.array(
              RespValue.bulkString("id1"),
              RespValue.bulkString("consumer1"),
              RespValue.Integer(101),
              RespValue.Integer(11)
            )
          )
          Task(PendingMessagesOutput.unsafeDecode(input)).map(
            assert(_)(
              hasSameElements(
                Chunk(
                  PendingMessage("id", "consumer", 100.millis, 10),
                  PendingMessage("id1", "consumer1", 101.millis, 11)
                )
              )
            )
          )
        },
        testM("error when message has more than four fields") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.bulkString("consumer"),
              RespValue.Integer(100),
              RespValue.Integer(10),
              RespValue.NullBulkString
            ),
            RespValue.array(
              RespValue.bulkString("id1"),
              RespValue.bulkString("consumer1"),
              RespValue.Integer(101),
              RespValue.Integer(11)
            )
          )
          Task(PendingMessagesOutput.unsafeDecode(input)).either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        testM("error when message has less than four fields") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.bulkString("consumer"),
              RespValue.Integer(100)
            ),
            RespValue.array(
              RespValue.bulkString("id1"),
              RespValue.bulkString("consumer1"),
              RespValue.Integer(101),
              RespValue.Integer(11)
            )
          )
          Task(PendingMessagesOutput.unsafeDecode(input)).either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("xRead")(
        testM("extract valid value") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("str1"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("id1"),
                  RespValue.array(
                    RespValue.bulkString("a"),
                    RespValue.bulkString("b")
                  )
                )
              )
            ),
            RespValue.array(
              RespValue.bulkString("str2"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("id2"),
                  RespValue.array(
                    RespValue.bulkString("c"),
                    RespValue.bulkString("d")
                  )
                ),
                RespValue.array(
                  RespValue.bulkString("id3"),
                  RespValue.array(
                    RespValue.bulkString("e"),
                    RespValue.bulkString("f")
                  )
                )
              )
            )
          )
          Task(
            KeyValueTwoOutput(ArbitraryOutput[String](), StreamOutput[String, String, String]()).unsafeDecode(input)
          ).map(
            assert(_)(
              equalTo(
                Map(
                  "str1" -> Map("id1" -> Map("a" -> "b")),
                  "str2" -> Map("id2" -> Map("c" -> "d"), "id3" -> Map("e" -> "f"))
                )
              )
            )
          )
        },
        testM("error when message content has odd number of fields") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("str1"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("id1"),
                  RespValue.array(
                    RespValue.bulkString("a")
                  )
                )
              )
            )
          )
          Task(
            KeyValueOutput(ArbitraryOutput[String](), StreamOutput[String, String, String]()).unsafeDecode(input)
          ).either.map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        testM("error when message doesn't have an ID") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("str1"),
              RespValue.array(
                RespValue.array(
                  RespValue.array(
                    RespValue.bulkString("a"),
                    RespValue.bulkString("b")
                  )
                )
              )
            )
          )
          Task(
            KeyValueOutput(ArbitraryOutput[String](), StreamOutput[String, String, String]()).unsafeDecode(input)
          ).either.map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        testM("error when stream doesn't have an ID") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("id1"),
                  RespValue.array(
                    RespValue.bulkString("a"),
                    RespValue.bulkString("b")
                  )
                )
              )
            )
          )
          Task(
            KeyValueOutput(ArbitraryOutput[String](), StreamOutput[String, String, String]()).unsafeDecode(input)
          ).either.map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        suite("xInfoStream")(
          testM("extract valid value with first and last entry") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("groups"),
              RespValue.Integer(2),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("2-0"),
              RespValue.bulkString("first-entry"),
              RespValue.array(
                RespValue.bulkString("1-0"),
                RespValue.array(
                  RespValue.bulkString("key1"),
                  RespValue.bulkString("value1")
                )
              ),
              RespValue.bulkString("last-entry"),
              RespValue.array(
                RespValue.bulkString("2-0"),
                RespValue.array(
                  RespValue.bulkString("key2"),
                  RespValue.bulkString("value2")
                )
              )
            )

            assertM(Task(StreamInfoOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(
                StreamInfo(
                  1,
                  2,
                  3,
                  2,
                  "2-0",
                  Some(StreamEntry("1-0", Map("key1" -> "value1"))),
                  Some(StreamEntry("2-0", Map("key2" -> "value2")))
                )
              )
            )
          },
          testM("extract valid value without first and last entry") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("groups"),
              RespValue.Integer(1),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("0-0")
            )

            assertM(Task(StreamInfoOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(StreamInfo[String, String, String](1, 2, 3, 1, "0-0", None, None))
            )
          }
        ),
        suite("xInfoGroups")(
          testM("extract a valid value") {
            val resp = RespValue.array(
              RespValue.array(
                RespValue.bulkString("name"),
                RespValue.bulkString("group1"),
                RespValue.bulkString("consumers"),
                RespValue.Integer(1),
                RespValue.bulkString("pending"),
                RespValue.Integer(1),
                RespValue.bulkString("last-delivered-id"),
                RespValue.bulkString("1-0")
              ),
              RespValue.array(
                RespValue.bulkString("name"),
                RespValue.bulkString("group2"),
                RespValue.bulkString("consumers"),
                RespValue.Integer(2),
                RespValue.bulkString("pending"),
                RespValue.Integer(2),
                RespValue.bulkString("last-delivered-id"),
                RespValue.bulkString("2-0")
              )
            )

            assertM(Task(StreamGroupsInfoOutput.unsafeDecode(resp)))(
              equalTo(
                Chunk(
                  StreamGroupsInfo("group1", 1, 1, "1-0"),
                  StreamGroupsInfo("group2", 2, 2, "2-0")
                )
              )
            )
          },
          testM("extract an empty array") {
            val resp = RespValue.array()

            assertM(Task(StreamGroupsInfoOutput.unsafeDecode(resp)))(isEmpty)
          }
        ),
        suite("xInfoConsumers")(
          testM("extract a valid value") {
            val resp = RespValue.array(
              RespValue.array(
                RespValue.bulkString("name"),
                RespValue.bulkString("consumer1"),
                RespValue.bulkString("pending"),
                RespValue.Integer(1),
                RespValue.bulkString("idle"),
                RespValue.Integer(100)
              ),
              RespValue.array(
                RespValue.bulkString("name"),
                RespValue.bulkString("consumer2"),
                RespValue.bulkString("pending"),
                RespValue.Integer(2),
                RespValue.bulkString("idle"),
                RespValue.Integer(200)
              )
            )

            assertM(Task(StreamConsumersInfoOutput.unsafeDecode(resp)))(
              equalTo(
                Chunk(
                  StreamConsumersInfo("consumer1", 1, 100.millis),
                  StreamConsumersInfo("consumer2", 2, 200.millis)
                )
              )
            )
          },
          testM("extract an empty array") {
            val resp = RespValue.array()

            assertM(Task(StreamConsumersInfoOutput.unsafeDecode(resp)))(isEmpty)
          }
        ),
        suite("xInfoStreamFull")(
          testM("extract a valid value without groups") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("0-0"),
              RespValue.bulkString("entries"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("3-0"),
                  RespValue.array(
                    RespValue.bulkString("key1"),
                    RespValue.bulkString("value1")
                  )
                ),
                RespValue.array(
                  RespValue.bulkString("4-0"),
                  RespValue.array(
                    RespValue.bulkString("key1"),
                    RespValue.bulkString("value1")
                  )
                )
              )
            )

            assertM(Task(StreamInfoFullOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(
                StreamInfoWithFull.FullStreamInfo(
                  1,
                  2,
                  3,
                  "0-0",
                  Chunk(StreamEntry("3-0", Map("key1" -> "value1")), StreamEntry("4-0", Map("key1" -> "value1"))),
                  Chunk.empty
                )
              )
            )
          },
          testM("extract a valid value without groups and entries") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("0-0")
            )
            assertM(Task(StreamInfoFullOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(
                StreamInfoWithFull.FullStreamInfo[String, String, String](1, 2, 3, "0-0", Chunk.empty, Chunk.empty)
              )
            )
          },
          testM("extract an empty array") {
            val resp = RespValue.array()
            assertM(Task(StreamConsumersInfoOutput.unsafeDecode(resp)))(isEmpty)
          },
          testM("extract a valid value with groups") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("0-0"),
              RespValue.bulkString("entries"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("3-0"),
                  RespValue.array(
                    RespValue.bulkString("key1"),
                    RespValue.bulkString("value1")
                  )
                ),
                RespValue.array(
                  RespValue.bulkString("4-0"),
                  RespValue.array(
                    RespValue.bulkString("key1"),
                    RespValue.bulkString("value1")
                  )
                )
              ),
              RespValue.bulkString("groups"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("name"),
                  RespValue.bulkString("name1"),
                  RespValue.bulkString("last-delivered-id"),
                  RespValue.bulkString("lastDeliveredId"),
                  RespValue.bulkString("pel-count"),
                  RespValue.Integer(1),
                  RespValue.bulkString("pending"),
                  RespValue.array(
                    RespValue.array(
                      RespValue.bulkString("entryId1"),
                      RespValue.bulkString("consumerName1"),
                      RespValue.Integer(1588152520299L),
                      RespValue.Integer(1)
                    ),
                    RespValue.array(
                      RespValue.bulkString("entryId2"),
                      RespValue.bulkString("consumerName2"),
                      RespValue.Integer(1588152520299L),
                      RespValue.Integer(1)
                    )
                  ),
                  RespValue.bulkString("consumers"),
                  RespValue.array(
                    RespValue.array(
                      RespValue.bulkString("name"),
                      RespValue.bulkString("Alice"),
                      RespValue.bulkString("seen-time"),
                      RespValue.Integer(1588152520299L),
                      RespValue.bulkString("pel-count"),
                      RespValue.Integer(1),
                      RespValue.bulkString("pending"),
                      RespValue.array(
                        RespValue.array(
                          RespValue.bulkString("entryId3"),
                          RespValue.Integer(1588152520299L),
                          RespValue.Integer(1)
                        )
                      )
                    )
                  )
                )
              )
            )

            assertM(Task(StreamInfoFullOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(
                StreamInfoWithFull.FullStreamInfo(
                  1,
                  2,
                  3,
                  "0-0",
                  Chunk(StreamEntry("3-0", Map("key1" -> "value1")), StreamEntry("4-0", Map("key1" -> "value1"))),
                  Chunk(
                    StreamInfoWithFull.ConsumerGroups(
                      "name1",
                      "lastDeliveredId",
                      1,
                      Chunk(
                        StreamInfoWithFull.GroupPel("entryId1", "consumerName1", 1588152520299L.millis, 1L),
                        StreamInfoWithFull.GroupPel("entryId2", "consumerName2", 1588152520299L.millis, 1L)
                      ),
                      Chunk(
                        StreamInfoWithFull.Consumers(
                          "Alice",
                          1588152520299L.millis,
                          1,
                          Chunk(
                            StreamInfoWithFull.ConsumerPel("entryId3", 1588152520299L.millis, 1)
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          }
        ),
        suite("ClientInfo")(
          testM("extract addresses") {
            val id          = 42L
            val inetAddress = InetAddress.getByName("127.0.0.1")
            val resp =
              RespValue.bulkString(s"addr=${inetAddress.getHostAddress} id=$id laddr=${inetAddress.getHostAddress}")
            for {
              res <- Task(ClientInfoOutput.unsafeDecode(resp))
            } yield assert(res)(
              equalTo(Chunk.single(ClientInfo(id = 42L, address = Some(inetAddress), localAddress = Some(inetAddress))))
            )
          },
          testM("extract flags") {
            import ClientFlag._
            val id   = 42L
            val resp = RespValue.bulkString(s"flags=bOPSRt id=$id")
            val expectedFlags: Set[ClientFlag] = Set(
              Blocked,
              MonitorMode,
              PubSub,
              Replica,
              TrackingTargetClientInvalid,
              KeysTrackingEnabled
            )
            for {
              res <- Task(ClientInfoOutput.unsafeDecode(resp))
            } yield assert(res)(equalTo(Chunk.single(ClientInfo(id = 42L, flags = expectedFlags))))
          },
          testM("ignore unknown flags") {
            val id   = 42L
            val resp = RespValue.bulkString(s"flags=XYZ id=$id")
            for {
              res <- Task(ClientInfoOutput.unsafeDecode(resp))
            } yield assert(res)(equalTo(Chunk.single(ClientInfo(id = id, flags = Set.empty))))
          },
          testM("extract multiple fields") {
            val resp = RespValue.bulkString(
              "sub=4 id=42 idle=6 fd=9234\nid=99 events=r db=33\nid=1 cmd=foo"
            )
            for {
              res <- Task(ClientInfoOutput.unsafeDecode(resp))
            } yield assert(res)(
              equalTo(
                Chunk(
                  ClientInfo(id = 42L, idle = Some(6.seconds), subscriptions = 4, fileDescriptor = Some(9234L)),
                  ClientInfo(id = 99L, events = ClientEvents(readable = true), databaseId = Some(33L)),
                  ClientInfo(id = 1L, lastCommand = Some("foo"))
                )
              )
            )
          }
        ),
        suite("ClientTrackingInfo")(
          testM("extract with flags set") {
            val resp = RespValue
              .array(
                RespValue.bulkString("flags"),
                RespValue.array(
                  RespValue.bulkString("on"),
                  RespValue.bulkString("optin"),
                  RespValue.bulkString("caching-yes"),
                  RespValue.bulkString("noloop")
                ),
                RespValue.bulkString("redirect"),
                RespValue.Integer(-1L),
                RespValue.bulkString("prefixes"),
                RespValue.NullArray
              )
            val expectedInfo = ClientTrackingInfo(
              ClientTrackingFlags(
                clientSideCaching = true,
                trackingMode = Some(ClientTrackingMode.OptIn),
                noLoop = true,
                caching = Some(true)
              )
            )
            for {
              res <- Task(ClientTrackingInfoOutput.unsafeDecode(resp))
            } yield assert(res)(equalTo(expectedInfo))
          },
          testM("extract with redirect id and broken redirect flag") {
            val resp = RespValue
              .array(
                RespValue.bulkString("flags"),
                RespValue.array(RespValue.bulkString("off"), RespValue.bulkString("broken_redirect")),
                RespValue.bulkString("redirect"),
                RespValue.Integer(42L),
                RespValue.bulkString("prefixes"),
                RespValue.NullArray
              )
            val expectedInfo = ClientTrackingInfo(
              ClientTrackingFlags(clientSideCaching = false, brokenRedirect = true),
              Some(42L)
            )
            for {
              res <- Task(ClientTrackingInfoOutput.unsafeDecode(resp))
            } yield assert(res)(equalTo(expectedInfo))
          },
          testM("extract with specified prefixes") {
            val resp = RespValue
              .array(
                RespValue.bulkString("flags"),
                RespValue.array(RespValue.bulkString("off")),
                RespValue.bulkString("redirect"),
                RespValue.Integer(-1L),
                RespValue.bulkString("prefixes"),
                RespValue.array(
                  RespValue.bulkString("prefix1"),
                  RespValue.bulkString("prefix2"),
                  RespValue.bulkString("prefix3")
                )
              )
            val expectedInfo = ClientTrackingInfo(
              ClientTrackingFlags(clientSideCaching = false),
              prefixes = Set("prefix1", "prefix2", "prefix3")
            )
            for {
              res <- Task(ClientTrackingInfoOutput.unsafeDecode(resp))
            } yield assert(res)(equalTo(expectedInfo))
          },
          testM("error when fields are missing") {
            val resp = RespValue
              .array(
                RespValue.bulkString("redirect"),
                RespValue.Integer(42L),
                RespValue.bulkString("prefixes"),
                RespValue.NullArray
              )
            Task(ClientTrackingInfoOutput.unsafeDecode(resp)).either
              .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
          }
        ),
        suite("ClientTrackingRedirect")(
          testM("extract not enabled") {
            val resp = RespValue.Integer(-1L)
            for {
              res <- Task(ClientTrackingRedirectOutput.unsafeDecode(resp))
            } yield assert(res)(equalTo(ClientTrackingRedirect.NotEnabled))
          },
          testM("extract not redirected") {
            val resp = RespValue.Integer(0L)
            for {
              res <- Task(ClientTrackingRedirectOutput.unsafeDecode(resp))
            } yield assert(res)(equalTo(ClientTrackingRedirect.NotRedirected))
          },
          testM("extract redirect id") {
            val resp = RespValue.Integer(42L)
            for {
              res <- Task(ClientTrackingRedirectOutput.unsafeDecode(resp))
            } yield assert(res)(equalTo(ClientTrackingRedirect.RedirectedTo(resp.value)))
          },
          testM("error when redirect id is invalid") {
            val resp = RespValue.Integer(-42L)
            Task(ClientTrackingRedirectOutput.unsafeDecode(resp)).either
              .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
          }
        )
      )
    )

  private val Noise = "bad input"
}
