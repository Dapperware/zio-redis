package zio.redis

import zio.Chunk
import zio.redis.RedisError.WrongType
import zio.test.Assertion._
import zio.test._

trait SetsSpec extends BaseSpec {
  val setsSuite =
    suite("sets")(
      suite("sAdd")(
        testM("to empty set") {
          for {
            key   <- uuid
            added <- sAdd(key, "hello")
          } yield assert(added)(equalTo(1L))
        },
        testM("to the non-empty set") {
          for {
            key   <- uuid
            _     <- sAdd(key, "hello")
            added <- sAdd(key, "world")
          } yield assert(added)(equalTo(1L))
        },
        testM("existing element to set") {
          for {
            key   <- uuid
            _     <- sAdd(key, "hello")
            added <- sAdd(key, "hello")
          } yield assert(added)(equalTo(0L))
        },
        testM("multiple elements to set") {
          for {
            key   <- uuid
            added <- sAdd(key, "a", "b", "c")
          } yield assert(added)(equalTo(3L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            added <- sAdd(key, "hello").either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sCard")(
        testM("non-empty set") {
          for {
            key  <- uuid
            _    <- sAdd(key, "hello")
            card <- sCard(key)
          } yield assert(card)(equalTo(1L))
        },
        testM("0 when key doesn't exist") {
          assertM(sCard("unknown"))(equalTo(0L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            card  <- sCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sDiff")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "a", "c")
            diff   <- sDiff(first, second)
          } yield assert(diff)(hasSameElements(Chunk("b", "d")))
        },
        testM("non-empty set and empty set") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- sAdd(nonEmpty, "a", "b")
            diff     <- sDiff(nonEmpty, empty)
          } yield assert(diff)(hasSameElements(Chunk("a", "b")))
        },
        testM("empty set and non-empty set") {
          for {
            empty    <- uuid
            nonEmpty <- uuid
            _        <- sAdd(nonEmpty, "a", "b")
            diff     <- sDiff(empty, nonEmpty)
          } yield assert(diff)(isEmpty)
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            diff   <- sDiff(first, second)
          } yield assert(diff)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "b", "d")
            _      <- sAdd(third, "b", "c")
            diff   <- sDiff(first, second, third)
          } yield assert(diff)(hasSameElements(Chunk("a")))
        },
        testM("error when first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value)
            diff   <- sDiff(first, second).either
          } yield assert(diff)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value)
            diff   <- sDiff(first, second).either
          } yield assert(diff)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sDiffStore")(
        testM("two non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "a", "c")
            card   <- sDiffStore(dest, first, second)
          } yield assert(card)(equalTo(2L))
        },
        testM("non-empty set and empty set") {
          for {
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- sAdd(nonEmpty, "a", "b")
            card     <- sDiffStore(dest, nonEmpty, empty)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty set and non-empty set") {
          for {
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- sAdd(nonEmpty, "a", "b")
            card     <- sDiffStore(dest, empty, nonEmpty)
          } yield assert(card)(equalTo(0L))
        },
        testM("empty when both sets are empty") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- sDiffStore(dest, first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "b", "d")
            _      <- sAdd(third, "b", "c")
            card   <- sDiffStore(dest, first, second, third)
          } yield assert(card)(equalTo(1L))
        },
        testM("error when first parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value)
            card   <- sDiffStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value)
            card   <- sDiffStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sInter")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "a", "c", "e")
            inter  <- sInter(first, second)
          } yield assert(inter)(hasSameElements(Chunk("a", "c")))
        },
        testM("empty when one of the sets is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- sAdd(nonEmpty, "a", "b")
            inter    <- sInter(nonEmpty, empty)
          } yield assert(inter)(isEmpty)
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            inter  <- sInter(first, second)
          } yield assert(inter)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "b", "d")
            _      <- sAdd(third, "b", "c")
            inter  <- sInter(first, second, third)
          } yield assert(inter)(hasSameElements(Chunk("b")))
        },
        testM("error when first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value)
            inter  <- sInter(first, second).either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("empty with empty first set and second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value)
            inter  <- sInter(first, second)
          } yield assert(inter)(isEmpty)
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- sAdd(first, "a")
            _      <- set(second, value)
            inter  <- sInter(first, second).either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sInterStore")(
        testM("two non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "a", "c", "e")
            card   <- sInterStore(dest, first, second)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when one of the sets is empty") {
          for {
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- sAdd(nonEmpty, "a", "b")
            card     <- sInterStore(dest, nonEmpty, empty)
          } yield assert(card)(equalTo(0L))
        },
        testM("empty when both sets are empty") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- sInterStore(dest, first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "b", "d")
            _      <- sAdd(third, "b", "c")
            card   <- sInterStore(dest, first, second, third)
          } yield assert(card)(equalTo(1L))
        },
        testM("error when first parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value)
            card   <- sInterStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("empty with empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value)
            card   <- sInterStore(dest, first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- sAdd(first, "a")
            _      <- set(second, value)
            card   <- sInterStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sIsMember")(
        testM("actual element of the non-empty set") {
          for {
            key      <- uuid
            _        <- sAdd(key, "a", "b", "c")
            isMember <- sIsMember(key, "b")
          } yield assert(isMember)(isTrue)
        },
        testM("element that is not present in the set") {
          for {
            key      <- uuid
            _        <- sAdd(key, "a", "b", "c")
            isMember <- sIsMember(key, "unknown")
          } yield assert(isMember)(isFalse)
        },
        testM("of an empty set") {
          for {
            key      <- uuid
            isMember <- sIsMember(key, "a")
          } yield assert(isMember)(isFalse)
        },
        testM("when not set") {
          for {
            key      <- uuid
            value    <- uuid
            _        <- set(key, value)
            isMember <- sIsMember(key, "a").either
          } yield assert(isMember)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sMembers")(
        testM("non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key, "a", "b", "c")
            members <- sMembers(key)
          } yield assert(members)(hasSameElements(Chunk("a", "b", "c")))
        },
        testM("empty set") {
          for {
            key     <- uuid
            members <- sMembers(key)
          } yield assert(members)(isEmpty)
        },
        testM("when not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value)
            members <- sMembers(key).either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sMove")(
        testM("from non-empty source to non-empty destination") {
          for {
            src   <- uuid
            dest  <- uuid
            _     <- sAdd(src, "a", "b", "c")
            _     <- sAdd(dest, "d", "e", "f")
            moved <- sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        testM("from non-empty source to empty destination") {
          for {
            src   <- uuid
            dest  <- uuid
            _     <- sAdd(src, "a", "b", "c")
            moved <- sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        testM("element already present in the destination") {
          for {
            src   <- uuid
            dest  <- uuid
            _     <- sAdd(src, "a", "b", "c")
            _     <- sAdd(dest, "a", "d", "e")
            moved <- sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        testM("from empty source to non-empty destination") {
          for {
            src   <- uuid
            dest  <- uuid
            _     <- sAdd(dest, "b", "c")
            moved <- sMove(src, dest, "a")
          } yield assert(moved)(isFalse)
        },
        testM("non-existent element") {
          for {
            src   <- uuid
            dest  <- uuid
            _     <- sAdd(src, "a", "b")
            _     <- sAdd(dest, "c", "d")
            moved <- sMove(src, dest, "unknown")
          } yield assert(moved)(isFalse)
        },
        testM("from empty source to not set destination") {
          for {
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- set(dest, value)
            moved <- sMove(src, dest, "unknown")
          } yield assert(moved)(isFalse)
        },
        testM("error when non-empty source and not set destination") {
          for {
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- sAdd(src, "a", "b", "c")
            _     <- set(dest, value)
            moved <- sMove(src, dest, "a").either
          } yield assert(moved)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when not set source to non-empty destination") {
          for {
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- set(src, value)
            _     <- sAdd(dest, "a", "b", "c")
            moved <- sMove(src, dest, "a").either
          } yield assert(moved)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sPop")(
        testM("one element from non-empty set") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a", "b", "c")
            popped <- sPop(key)
          } yield assert(popped)(isNonEmpty)
        },
        testM("one element from an empty set") {
          for {
            key    <- uuid
            popped <- sPop(key)
          } yield assert(popped)(isEmpty)
        },
        testM("error when poping one element from not set") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            popped <- sPop(key).either
          } yield assert(popped)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("multiple elements from non-empty set") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a", "b", "c")
            popped <- sPop(key, Some(2L))
          } yield assert(popped)(hasSize(equalTo(2)))
        },
        testM("more elements then there is in non-empty set") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a", "b", "c")
            popped <- sPop(key, Some(5L))
          } yield assert(popped)(hasSize(equalTo(3)))
        },
        testM("multiple elements from empty set") {
          for {
            key    <- uuid
            popped <- sPop(key, Some(3))
          } yield assert(popped)(isEmpty)
        },
        testM("error when poping multiple elements from not set") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            popped <- sPop(key, Some(3)).either
          } yield assert(popped)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sRandMember")(
        testM("one element from non-empty set") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a", "b", "c")
            member <- sRandMember(key)
          } yield assert(member)(hasSize(equalTo(1)))
        },
        testM("one element from an empty set") {
          for {
            key    <- uuid
            member <- sRandMember(key)
          } yield assert(member)(isEmpty)
        },
        testM("error when one element from not set") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            member <- sRandMember(key).either
          } yield assert(member)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("multiple elements from non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key, "a", "b", "c")
            members <- sRandMember(key, Some(2L))
          } yield assert(members)(hasSize(equalTo(2)))
        },
        testM("more elements than there is present in the non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key, "a", "b", "c")
            members <- sRandMember(key, Some(5L))
          } yield assert(members)(hasSize(equalTo(3)))
        },
        testM("multiple elements from an empty set") {
          for {
            key     <- uuid
            members <- sRandMember(key, Some(3L))
          } yield assert(members)(isEmpty)
        },
        testM("repeated elements from non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key, "a", "b", "c")
            members <- sRandMember(key, Some(-5L))
          } yield assert(members)(hasSize(equalTo(5)))
        },
        testM("repeated elements from an empty set") {
          for {
            key     <- uuid
            members <- sRandMember(key, Some(-3L))
          } yield assert(members)(isEmpty)
        },
        testM("error multiple elements from not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value)
            members <- sRandMember(key, Some(3L)).either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ testExecutorUnsupported,
      suite("sRem")(
        testM("existing elements from non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key, "a", "b", "c")
            removed <- sRem(key, "b", "c")
          } yield assert(removed)(equalTo(2L))
        },
        testM("when just part of elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key, "a", "b", "c")
            removed <- sRem(key, "b", "d")
          } yield assert(removed)(equalTo(1L))
        },
        testM("when none of the elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key, "a", "b", "c")
            removed <- sRem(key, "d", "e")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from an empty set") {
          for {
            key     <- uuid
            removed <- sRem(key, "a", "b")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value)
            removed <- sRem(key, "a", "b").either
          } yield assert(removed)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ testExecutorUnsupported,
      suite("sUnion")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "a", "c", "e")
            union  <- sUnion(first, second)
          } yield assert(union)(hasSameElements(Chunk("a", "b", "c", "d", "e")))
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- sAdd(nonEmpty, "a", "b")
            union    <- sUnion(nonEmpty, empty)
          } yield assert(union)(hasSameElements(Chunk("a", "b")))
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            union  <- sUnion(first, second)
          } yield assert(union)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "b", "d")
            _      <- sAdd(third, "b", "c", "e")
            union  <- sUnion(first, second, third)
          } yield assert(union)(hasSameElements(Chunk("a", "b", "c", "d", "e")))
        },
        testM("error when first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value)
            union  <- sUnion(first, second).either
          } yield assert(union)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- sAdd(first, "a")
            _      <- set(second, value)
            union  <- sUnion(first, second).either
          } yield assert(union)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ testExecutorUnsupported,
      suite("sUnionStore")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "a", "c", "e")
            card   <- sUnionStore(dest, first, second)
          } yield assert(card)(equalTo(5L))
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            dest     <- uuid
            _        <- sAdd(nonEmpty, "a", "b")
            card     <- sUnionStore(dest, nonEmpty, empty)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            card   <- sUnionStore(dest, first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            dest   <- uuid
            _      <- sAdd(first, "a", "b", "c", "d")
            _      <- sAdd(second, "b", "d")
            _      <- sAdd(third, "b", "c", "e")
            card   <- sUnionStore(dest, first, second, third)
          } yield assert(card)(equalTo(5L))
        },
        testM("error when the first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- set(first, value)
            card   <- sUnionStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- sAdd(first, "a")
            _      <- set(second, value)
            card   <- sUnionStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ testExecutorUnsupported,
      suite("sScan")(
        testM("non-empty set") {
          for {
            key              <- uuid
            _                <- sAdd(key, "a", "b", "c")
            scan             <- sScan(key, 0L)
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("empty set") {
          for {
            key              <- uuid
            scan             <- sScan(key, 0L)
            (cursor, members) = scan
          } yield assert(cursor)(equalTo("0")) &&
            assert(members)(isEmpty)
        },
        testM("with match over non-empty set") {
          for {
            key              <- uuid
            _                <- sAdd(key, "one", "two", "three")
            scan             <- sScan(key, 0L, Some("t[a-z]*".r))
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("with count over non-empty set") {
          for {
            key              <- uuid
            _                <- sAdd(key, "a", "b", "c", "d", "e")
            scan             <- sScan(key, 0L, count = Some(Count(3L)))
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            scan  <- sScan(key, 0L).either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ testExecutorUnsupported
    )
}
