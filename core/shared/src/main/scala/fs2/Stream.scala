/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2

import scala.annotation.{nowarn, tailrec}
import scala.concurrent.TimeoutException
import scala.concurrent.duration._

import cats.{Eval => _, _}
import cats.data.Ior
import cats.effect.{Concurrent, SyncIO}
import cats.effect.kernel._
import cats.effect.kernel.implicits._
import cats.effect.std.{Console, Queue, QueueSink, QueueSource, Semaphore}
import cats.effect.Resource.ExitCase
import cats.syntax.all._

import fs2.compat._
import fs2.concurrent._
import fs2.internal._

import scala.collection.mutable.ArrayBuffer
import cats.data.NonEmptyList

/** A stream producing output of type `O` and which may evaluate `F` effects.
  *
  * - '''Purely functional''' a value of type `Stream[F, O]` _describes_ an effectful computation.
  *    A function that returns a `Stream[F, O]` builds a _description_ of an effectful computation,
  *    but does not perform them. The methods of the `Stream` class derive new descriptions from others.
  *    This is similar to how effect types like `cats.effect.IO` and `monix.Task` build descriptions of
  *    computations.
  *
  * - '''Pull''': to evaluate a stream, a consumer pulls its values from it, by repeatedly performing one pull step at a time.
  *   Each step is a `F`-effectful computation that may yield some `O` values (or none), and a stream from which to continue pulling.
  *   The consumer controls the evaluation of the stream, which effectful operations are performed, and when.
  *
  * - '''Non-Strict''': stream evaluation only pulls from the stream a prefix large enough to compute its results.
  *   Thus, although a stream may yield an unbounded number of values or, after successfully yielding several values,
  *   either raise an error or hang up and never yield any value, the consumer need not reach those points of failure.
  *   For the same reason, in general, no effect in `F` is evaluated unless and until the consumer needs it.
  *
  * - '''Abstract''': a stream needs not be a plain finite list of fixed effectful computations in F.
  *   It can also represent an input or output connection through which data incrementally arrives.
  *   It can represent an effectful computation, such as reading the system's time, that can be re-evaluated
  *   as often as the consumer of the stream requires.
  *
  * === Special properties for streams ===
  *
  * There are some special properties or cases of streams:
  *  - A stream is '''finite''' if we can reach the end after a limited number of pull steps,
  *    which may yield a finite number of values. It is '''empty''' if it terminates and yields no values.
  *  - A '''singleton''' stream is a stream that ends after yielding one single value.
  *  - A '''pure''' stream is one in which the `F` is [[Pure]], which indicates that it evaluates no effects.
  *  - A '''never''' stream is a stream that never terminates and never yields any value.
  *
  * == Pure Streams and operations ==
  *
  * We can sometimes think of streams, naively, as lists of `O` elements with `F`-effects.
  * This is particularly true for '''pure''' streams, which are instances of `Stream` which use the [[Pure]] effect type.
  * We can convert every ''pure and finite'' stream into a `List[O]` using the `.toList` method.
  * Also, we can convert pure ''infinite'' streams into instances of the `Stream[O]` class from the Scala standard library.
  *
  * A method of the `Stream` class is '''pure''' if it can be applied to pure streams. Such methods are identified
  * in that their signature includes no type-class constraint (or implicit parameter) on the `F` method.
  * Pure methods in `Stream[F, O]` can be projected ''naturally'' to methods in the `List` class, which means
  * that applying the stream's method and converting the result to a list gets the same result as
  * first converting the stream to a list, and then applying list methods.
  *
  * Some methods that project directly to list are `map`, `filter`, `takeWhile`, etc.
  * There are other methods, like `exists` or `find`, that in the `List` class they return a value or an `Option`,
  * but their stream counterparts return an (either empty or singleton) stream.
  * Other methods, like `zipWithPrevious`, have a more complicated but still pure translation to list methods.
  *
  * == Type-Class instances and laws of the Stream Operations ==
  *
  * Laws (using infix syntax):
  *
  * `append` forms a monoid in conjunction with `empty`:
  *   - `empty append s == s` and `s append empty == s`.
  *   - `(s1 append s2) append s3 == s1 append (s2 append s3)`
  *
  * And `cons` is consistent with using `++` to prepend a single chunk:
  *   - `s.cons(c) == Stream.chunk(c) ++ s`
  *
  * `Stream.raiseError` propagates until being caught by `handleErrorWith`:
  *   - `Stream.raiseError(e) handleErrorWith h == h(e)`
  *   - `Stream.raiseError(e) ++ s == Stream.raiseError(e)`
  *   - `Stream.raiseError(e) flatMap f == Stream.raiseError(e)`
  *
  * `Stream` forms a monad with `emit` and `flatMap`:
  *   - `Stream.emit >=> f == f` (left identity)
  *   - `f >=> Stream.emit === f` (right identity - note weaker equality notion here)
  *   - `(f >=> g) >=> h == f >=> (g >=> h)` (associativity)
  *  where `Stream.emit(a)` is defined as `chunk(Chunk.singleton(a)) and
  *  `f >=> g` is defined as `a => a flatMap f flatMap g`
  *
  * The monad is the list-style sequencing monad:
  *   - `(a ++ b) flatMap f == (a flatMap f) ++ (b flatMap f)`
  *   - `Stream.empty flatMap f == Stream.empty`
  *
  * == Technical notes==
  *
  * ''Note:'' since the chunk structure of the stream is observable, and
  * `s flatMap Stream.emit` produces a stream of singleton chunks,
  * the right identity law uses a weaker notion of equality, `===` which
  * normalizes both sides with respect to chunk structure:
  *
  *   `(s1 === s2) = normalize(s1) == normalize(s2)`
  *   where `==` is full equality
  *   (`a == b` iff `f(a)` is identical to `f(b)` for all `f`)
  *
  * `normalize(s)` can be defined as `s.flatMap(Stream.emit)`, which just
  * produces a singly-chunked stream from any input stream `s`.
  *
  * For instance, for a stream `s` and a function `f: A => B`,
  * - the result of `s.map(f)` is a Stream with the same _chunking_ as the `s`; whereas...
  * - the result of `s.flatMap(x => S.emit(f(x)))` is a Stream structured as a sequence of singleton chunks.
  * The latter is using the definition of `map` that is derived from the `Monad` instance.
  *
  * This is not unlike equality for maps or sets, which is defined by which elements they contain,
  * not by how these are spread between a tree's branches or a hashtable buckets.
  * However, a `Stream` structure can be _observed_ through the `chunks` method,
  * so two streams "_equal_" under that notion may give different results through this method.
  *
  * ''Note:'' For efficiency `[[Stream.map]]` function operates on an entire
  * chunk at a time and preserves chunk structure, which differs from
  * the `map` derived from the monad (`s map f == s flatMap (f andThen Stream.emit)`)
  * which would produce singleton chunk. In particular, if `f` throws errors, the
  * chunked version will fail on the first ''chunk'' with an error, while
  * the unchunked version will fail on the first ''element'' with an error.
  * Exceptions in pure code like this are strongly discouraged.
  *
  * @hideImplicitConversion PureOps
  * @hideImplicitConversion IdOps
  */
final class Stream[+F[_], +O] private[fs2] (private[fs2] val underlying: Pull[F, O, Unit]) {

  /** Appends `s2` to the end of this stream.
    *
    * @example {{{
    * scala> (Stream(1,2,3) ++ Stream(4,5,6)).toList
    * res0: List[Int] = List(1, 2, 3, 4, 5, 6)
    * }}}
    *
    * If `this` stream is infinite, then the result is equivalent to `this`.
    */
  def ++[F2[x] >: F[x], O2 >: O](s2: => Stream[F2, O2]): Stream[F2, O2] =
    new Stream(underlying >> s2.underlying)

  /** Appends `s2` to the end of this stream. Alias for `s1 ++ s2`. */
  def append[F2[x] >: F[x], O2 >: O](s2: => Stream[F2, O2]): Stream[F2, O2] =
    this ++ s2

  /** Equivalent to `val o2Memoized = o2; _.map(_ => o2Memoized)`.
    *
    * @example {{{
    * scala> Stream(1,2,3).as(0).toList
    * res0: List[Int] = List(0, 0, 0)
    * }}}
    */
  def as[O2](o2: O2): Stream[F, O2] = map(_ => o2)

  /** Returns a stream of `O` values wrapped in `Right` until the first error, which is emitted wrapped in `Left`.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> (Stream(1,2,3) ++ Stream.raiseError[SyncIO](new RuntimeException) ++ Stream(4,5,6)).attempt.compile.toList.unsafeRunSync()
    * res0: List[Either[Throwable,Int]] = List(Right(1), Right(2), Right(3), Left(java.lang.RuntimeException))
    * }}}
    *
    * [[rethrow]] is the inverse of `attempt`, with the caveat that anything after the first failure is discarded.
    */
  def attempt: Stream[F, Either[Throwable, O]] =
    map(Right(_): Either[Throwable, O]).handleErrorWith(e => Stream.emit(Left(e)))

  /** Retries on failure, returning a stream of attempts that can
    * be manipulated with standard stream operations such as `take`,
    * `collectFirst` and `interruptWhen`.
    *
    * Note: The resulting stream does *not* automatically halt at the
    * first successful attempt. Also see `retry`.
    */
  def attempts[F2[x] >: F[x]: Temporal](
      delays: Stream[F2, FiniteDuration]
  ): Stream[F2, Either[Throwable, O]] =
    attempt ++ delays.flatMap(delay => Stream.sleep_(delay) ++ attempt)

  /** Broadcasts every value of the stream through the pipes provided
    * as arguments.
    *
    * Each pipe can have a different implementation if required, and
    * they are all guaranteed to see every `O` pulled from the source
    * stream.
    *
    * The pipes are all run concurrently with each other, but note
    * that elements are pulled from the source as chunks, and the next
    * chunk is pulled only when all pipes are done with processing the
    * current chunk, which prevents faster pipes from getting too far ahead.
    *
    * In other words, this behaviour slows down processing of incoming
    * chunks by faster pipes until the slower ones have caught up. If
    * this is not desired, consider using the `prefetch` and
    * `prefetchN` combinators on the slow pipes.
    */
  def broadcastThrough[F2[x] >: F[x]: Concurrent, O2](
      pipes: Pipe[F2, O, O2]*
  ): Stream[F2, O2] = {
    assert(pipes.nonEmpty, s"pipes should not be empty")
    Stream
      .eval {
        (
          cats.effect.std.CountDownLatch[F2](pipes.length),
          fs2.concurrent.Topic[F2, Chunk[O]]
        ).tupled
      }
      .flatMap { case (latch, topic) =>
        def produce = chunks.through(topic.publish)

        def consume(pipe: Pipe[F2, O, O2]): Pipe[F2, Chunk[O], O2] =
          _.flatMap(Stream.chunk).through(pipe)

        Stream(pipes: _*)
          .map { pipe =>
            Stream
              .resource(topic.subscribeAwait(1))
              .flatMap { sub =>
                // crucial that awaiting on the latch is not passed to
                // the pipe, so that the pipe cannot interrupt it and alter
                // the latch count
                Stream.exec(latch.release >> latch.await) ++ sub.through(consume(pipe))
              }
          }
          .parJoinUnbounded
          .concurrently(Stream.eval(latch.await) ++ produce)
      }
  }

  /** Behaves like the identity function, but requests `n` elements at a time from the input.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> val buf = new scala.collection.mutable.ListBuffer[String]()
    * scala> Stream.range(0, 100).covary[SyncIO].
    *      |   evalMap(i => SyncIO { buf += s">$i"; i }).
    *      |   buffer(4).
    *      |   evalMap(i => SyncIO { buf += s"<$i"; i }).
    *      |   take(10).
    *      |   compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    * scala> buf.toList
    * res1: List[String] = List(>0, >1, >2, >3, <0, <1, <2, <3, >4, >5, >6, >7, <4, <5, <6, <7, >8, >9, >10, >11, <8, <9)
    * }}}
    */
  def buffer(n: Int): Stream[F, O] =
    if (n <= 0) this
    else
      this.repeatPull {
        _.unconsN(n, allowFewer = true).flatMap {
          case Some((hd, tl)) => Pull.output(hd).as(Some(tl))
          case None           => Pull.pure(None)
        }
      }

  /** Behaves like the identity stream, but emits no output until the source is exhausted.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> val buf = new scala.collection.mutable.ListBuffer[String]()
    * scala> Stream.range(0, 10).covary[SyncIO].
    *      |   evalMap(i => SyncIO { buf += s">$i"; i }).
    *      |   bufferAll.
    *      |   evalMap(i => SyncIO { buf += s"<$i"; i }).
    *      |   take(4).
    *      |   compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(0, 1, 2, 3)
    * scala> buf.toList
    * res1: List[String] = List(>0, >1, >2, >3, >4, >5, >6, >7, >8, >9, <0, <1, <2, <3)
    * }}}
    */
  def bufferAll: Stream[F, O] = bufferBy(_ => true)

  /** Behaves like the identity stream, but requests elements from its
    * input in blocks that end whenever the predicate switches from true to false.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> val buf = new scala.collection.mutable.ListBuffer[String]()
    * scala> Stream.range(0, 10).covary[SyncIO].
    *      |   evalMap(i => SyncIO { buf += s">$i"; i }).
    *      |   bufferBy(_ % 2 == 0).
    *      |   evalMap(i => SyncIO { buf += s"<$i"; i }).
    *      |   compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    * scala> buf.toList
    * res1: List[String] = List(>0, >1, <0, <1, >2, >3, <2, <3, >4, >5, <4, <5, >6, >7, <6, <7, >8, >9, <8, <9)
    * }}}
    */
  def bufferBy(f: O => Boolean): Stream[F, O] = {
    def dumpBuffer(bb: List[Chunk[O]]): Pull[F, O, Unit] =
      bb.reverse.foldLeft(Pull.done: Pull[F, O, Unit])((acc, c) => acc >> Pull.output(c))

    def go(buffer: List[Chunk[O]], last: Boolean, s: Stream[F, O]): Pull[F, O, Unit] =
      s.pull.uncons.flatMap {
        case Some((hd, tl)) =>
          val (out, buf, newLast) =
            hd.foldLeft((Nil: List[Chunk[O]], Vector.empty[O], last)) {
              case ((out, buf, last), i) =>
                val cur = f(i)
                if (!cur && last)
                  (Chunk.vector(buf :+ i) :: out, Vector.empty, cur)
                else (out, buf :+ i, cur)
            }
          if (out.isEmpty)
            go(Chunk.vector(buf) :: buffer, newLast, tl)
          else
            dumpBuffer(buffer) >> dumpBuffer(out) >> go(List(Chunk.vector(buf)), newLast, tl)

        case None => dumpBuffer(buffer)
      }
    go(Nil, false, this).stream
  }

  /** Emits only elements that are distinct from their immediate predecessors,
    * using natural equality for comparison.
    *
    * @example {{{
    * scala> Stream(1,1,2,2,2,3,3).changes.toList
    * res0: List[Int] = List(1, 2, 3)
    * }}}
    */
  def changes[O2 >: O](implicit eq: Eq[O2]): Stream[F, O2] =
    filterWithPrevious(eq.neqv)

  /** Emits only elements that are distinct from their immediate predecessors
    * according to `f`, using natural equality for comparison.
    *
    * Note that `f` is called for each element in the stream multiple times
    * and hence should be fast (e.g., an accessor). It is not intended to be
    * used for computationally intensive conversions. For such conversions,
    * consider something like: `src.map(o => (o, f(o))).changesBy(_._2).map(_._1)`
    *
    * @example {{{
    * scala> Stream(1,1,2,4,6,9).changesBy(_ % 2).toList
    * res0: List[Int] = List(1, 2, 9)
    * }}}
    */
  def changesBy[O2](f: O => O2)(implicit eq: Eq[O2]): Stream[F, O] =
    filterWithPrevious((o1, o2) => eq.neqv(f(o1), f(o2)))

  /** Collects all output chunks in to a single chunk and emits it at the end of the
    * source stream. Note: if more than 2^32-1 elements are collected, this operation
    * will fail.
    *
    * @example {{{
    * scala> (Stream(1) ++ Stream(2, 3) ++ Stream(4, 5, 6)).chunkAll.toList
    * res0: List[Chunk[Int]] = List(Chunk(1, 2, 3, 4, 5, 6))
    * }}}
    */
  def chunkAll: Stream[F, Chunk[O]] = {
    def loop(s: Stream[F, O], acc: Chunk[O]): Pull[F, Chunk[O], Unit] =
      s.pull.uncons.flatMap {
        case Some((hd, tl)) => loop(tl, acc ++ hd)
        case None           => Pull.output1(acc)
      }
    loop(this, Chunk.empty).stream
  }

  /** Outputs all chunks from the source stream.
    *
    * @example {{{
    * scala> (Stream(1) ++ Stream(2, 3) ++ Stream(4, 5, 6)).chunks.toList
    * res0: List[Chunk[Int]] = List(Chunk(1), Chunk(2, 3), Chunk(4, 5, 6))
    * }}}
    */
  def chunks: Stream[F, Chunk[O]] =
    this.repeatPull(_.uncons.flatMap {
      case None           => Pull.pure(None)
      case Some((hd, tl)) => Pull.output1(hd).as(Some(tl))
    })

  /** Outputs chunk with a limited maximum size, splitting as necessary.
    *
    * @example {{{
    * scala> (Stream(1) ++ Stream(2, 3) ++ Stream(4, 5, 6)).chunkLimit(2).toList
    * res0: List[Chunk[Int]] = List(Chunk(1), Chunk(2, 3), Chunk(4, 5), Chunk(6))
    * }}}
    */
  def chunkLimit(n: Int): Stream[F, Chunk[O]] =
    this.repeatPull {
      _.unconsLimit(n).flatMap {
        case None           => Pull.pure(None)
        case Some((hd, tl)) => Pull.output1(hd).as(Some(tl))
      }
    }

  /** Outputs chunks of size larger than N
    *
    * Chunks from the source stream are split as necessary.
    *
    * If `allowFewerTotal` is true,
    * if the stream is smaller than N, should the elements be included
    *
    * @example {{{
    * scala> (Stream(1,2) ++ Stream(3,4) ++ Stream(5,6,7)).chunkMin(3).toList
    * res0: List[Chunk[Int]] = List(Chunk(1, 2, 3, 4), Chunk(5, 6, 7))
    * }}}
    */
  def chunkMin(n: Int, allowFewerTotal: Boolean = true): Stream[F, Chunk[O]] = {
    // Untyped Guarantee: accFull.size >= n | accFull.size == 0
    def go[A](nextChunk: Chunk[A], s: Stream[F, A]): Pull[F, Chunk[A], Unit] =
      s.pull.uncons.flatMap {
        case None =>
          if (allowFewerTotal && nextChunk.size > 0)
            Pull.output1(nextChunk)
          else
            Pull.done
        case Some((hd, tl)) =>
          val next = nextChunk ++ hd
          if (next.size >= n)
            Pull.output1(next) >> go(Chunk.empty, tl)
          else
            go(next, tl)
      }

    this.pull.uncons.flatMap {
      case None => Pull.done
      case Some((hd, tl)) =>
        if (hd.size >= n)
          Pull.output1(hd) >> go(Chunk.empty, tl)
        else go(hd, tl)
    }.stream
  }

  /** Outputs chunks of size `n`.
    *
    * Chunks from the source stream are split as necessary.
    * If `allowFewer` is true, the last chunk that is emitted may have less than `n` elements.
    *
    * @example {{{
    * scala> Stream(1,2,3).repeat.chunkN(2).take(5).toList
    * res0: List[Chunk[Int]] = List(Chunk(1, 2), Chunk(3, 1), Chunk(2, 3), Chunk(1, 2), Chunk(3, 1))
    * }}}
    */
  def chunkN(n: Int, allowFewer: Boolean = true): Stream[F, Chunk[O]] =
    this.repeatPull {
      _.unconsN(n, allowFewer).flatMap {
        case Some((hd, tl)) => Pull.output1(hd).as(Some(tl))
        case None           => Pull.pure(None)
      }
    }

  /** Filters and maps simultaneously. Calls `collect` on each chunk in the stream.
    *
    * @example {{{
    * scala> Stream(Some(1), Some(2), None, Some(3), None, Some(4)).collect { case Some(i) => i }.toList
    * res0: List[Int] = List(1, 2, 3, 4)
    * }}}
    */
  def collect[O2](pf: PartialFunction[O, O2]): Stream[F, O2] =
    mapChunks(_.collect(pf))

  /** Emits the first element of the stream for which the partial function is defined.
    *
    * @example {{{
    * scala> Stream(None, Some(1), Some(2), None, Some(3)).collectFirst { case Some(i) => i }.toList
    * res0: List[Int] = List(1)
    * }}}
    */
  def collectFirst[O2](pf: PartialFunction[O, O2]): Stream[F, O2] =
    this.pull
      .find(pf.isDefinedAt)
      .flatMap {
        case None          => Pull.done
        case Some((hd, _)) => Pull.output1(pf(hd))
      }
      .stream

  /** Like [[collect]] but terminates as soon as the partial function is undefined.
    *
    * @example {{{
    * scala> Stream(Some(1), Some(2), Some(3), None, Some(4)).collectWhile { case Some(i) => i }.toList
    * res0: List[Int] = List(1, 2, 3)
    * }}}
    */
  def collectWhile[O2](pf: PartialFunction[O, O2]): Stream[F, O2] =
    takeWhile(pf.isDefinedAt).map(pf)

  /** Gets a projection of this stream that allows converting it to an `F[..]` in a number of ways.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> val prg: SyncIO[Vector[Int]] = Stream.eval(SyncIO(1)).append(Stream(2,3,4)).compile.toVector
    * scala> prg.unsafeRunSync()
    * res2: Vector[Int] = Vector(1, 2, 3, 4)
    * }}}
    */
  def compile[F2[x] >: F[x], G[_], O2 >: O](implicit
      compiler: Compiler[F2, G]
  ): Stream.CompileOps[F2, G, O2] =
    new Stream.CompileOps[F2, G, O2](underlying)

  /** Runs the supplied stream in the background as elements from this stream are pulled.
    *
    * The resulting stream terminates upon termination of this stream. The background stream will
    * be interrupted at that point. Early termination of `that` does not terminate the resulting stream.
    *
    * Any errors that occur in either `this` or `that` stream result in the overall stream terminating
    * with an error.
    *
    * Upon finalization, the resulting stream will interrupt the background stream and wait for it to be
    * finalized.
    *
    * This method is equivalent to `this mergeHaltL that.drain`, just more efficient for `this` and `that` evaluation.
    *
    * @example {{{
    * scala> import cats.effect.IO, cats.effect.unsafe.implicits.global
    * scala> val data: Stream[IO,Int] = Stream.range(1, 10).covary[IO]
    * scala> Stream.eval(fs2.concurrent.SignallingRef[IO,Int](0)).flatMap(s => Stream(s).concurrently(data.evalMap(s.set))).flatMap(_.discrete).takeWhile(_ < 9, true).compile.last.unsafeRunSync()
    * res0: Option[Int] = Some(9)
    * }}}
    */
  def concurrently[F2[x] >: F[x], O2](
      that: Stream[F2, O2]
  )(implicit F: Concurrent[F2]): Stream[F2, O] = {
    val fstream: F2[Stream[F2, O]] = for {
      interrupt <- F.deferred[Unit]
      backResult <- F.deferred[Either[Throwable, Unit]]
    } yield {
      def watch[A](str: Stream[F2, A]) = str.interruptWhen(interrupt.get.attempt)

      val compileBack: F2[Boolean] = watch(that).compile.drain.attempt.flatMap {
        // Pass the result of backstream completion in the backResult deferred.
        // IF result of back-stream was failed, interrupt fore. Otherwise, let it be
        case r @ Right(_) => backResult.complete(r)
        case l @ Left(_)  => backResult.complete(l) >> interrupt.complete(())
      }

      // stop background process but await for it to finalise with a result
      // We use F.fromEither to bring errors from the back into the fore
      val stopBack: F2[Unit] = interrupt.complete(()) >> backResult.get.flatMap(F.fromEither)

      Stream.bracket(compileBack.start)(_ => stopBack) >> watch(this)
    }

    Stream.eval(fstream).flatten
  }

  /** Prepends a chunk onto the front of this stream.
    *
    * @example {{{
    * scala> Stream(1,2,3).cons(Chunk(-1, 0)).toList
    * res0: List[Int] = List(-1, 0, 1, 2, 3)
    * }}}
    */
  def cons[O2 >: O](c: Chunk[O2]): Stream[F, O2] =
    if (c.isEmpty) this else Stream.chunk(c) ++ this

  /** Prepends a chunk onto the front of this stream.
    *
    * @example {{{
    * scala> Stream(1,2,3).consChunk(Chunk.vector(Vector(-1, 0))).toList
    * res0: List[Int] = List(-1, 0, 1, 2, 3)
    * }}}
    */
  def consChunk[O2 >: O](c: Chunk[O2]): Stream[F, O2] =
    cons(c)

  /** Prepends a single value onto the front of this stream.
    *
    * @example {{{
    * scala> Stream(1,2,3).cons1(0).toList
    * res0: List[Int] = List(0, 1, 2, 3)
    * }}}
    */
  def cons1[O2 >: O](o: O2): Stream[F, O2] =
    cons(Chunk.singleton(o))

  /** Lifts this stream to the specified effect and output types.
    *
    * @example {{{
    * scala> import cats.effect.IO
    * scala> Stream.empty.covaryAll[IO,Int]
    * res0: Stream[IO,Int] = Stream(..)
    * }}}
    */
  def covaryAll[F2[x] >: F[x], O2 >: O]: Stream[F2, O2] = this

  /** Lifts this stream to the specified output type.
    *
    * @example {{{
    * scala> Stream(Some(1), Some(2), Some(3)).covaryOutput[Option[Int]]
    * res0: Stream[Pure,Option[Int]] = Stream(..)
    * }}}
    */
  def covaryOutput[O2 >: O]: Stream[F, O2] = this

  /** Debounce the stream with a minimum period of `d` between each element.
    *
    * Use-case: if this is a stream of updates about external state, we may
    * want to refresh (side-effectful) once every 'd' milliseconds, and every
    * time we refresh we only care about the latest update.
    *
    * @return A stream whose values is an in-order, not necessarily strict
    * subsequence of this stream, and whose evaluation will force a delay
    * `d` between emitting each element. The exact subsequence would depend
    * on the chunk structure of this stream, and the timing they arrive.
    *
    * @example {{{
    * scala> import scala.concurrent.duration._, cats.effect.IO, cats.effect.unsafe.implicits.global
    * scala> val s = Stream(1, 2, 3) ++ Stream.sleep_[IO](500.millis) ++ Stream(4, 5) ++ Stream.sleep_[IO](10.millis) ++ Stream(6)
    * scala> val s2 = s.debounce(100.milliseconds)
    * scala> s2.compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(3, 6)
    * }}}
    */
  def debounce[F2[x] >: F[x]](
      d: FiniteDuration
  )(implicit F: Temporal[F2]): Stream[F2, O] = Stream.force {
    for {
      chan <- Channel.bounded[F2, O](1)
      ref <- F.ref[Option[O]](None)
    } yield {
      val sendLatest: F2[Unit] =
        ref.getAndSet(None).flatMap(_.traverse_(chan.send))

      def sendItem(o: O): F2[Unit] =
        ref.getAndSet(Some(o)).flatMap {
          case None    => (F.sleep(d) >> sendLatest).start.void
          case Some(_) => F.unit
        }

      def go(tl: Pull[F2, O, Unit]): Pull[F2, INothing, Unit] =
        Pull.uncons(tl).flatMap {
          // Note: hd is non-empty, so hd.last.get is safe
          case Some((hd, tl)) => Pull.eval(sendItem(hd.last.get)) >> go(tl)
          case None           => Pull.eval(sendLatest >> chan.close.void)
        }

      val debouncedSend: Stream[F2, INothing] = new Stream(go(this.underlying))

      chan.stream.concurrently(debouncedSend)
    }
  }

  /** Throttles the stream to the specified `rate`. Unlike [[debounce]], [[metered]] doesn't drop elements.
    *
    * Provided `rate` should be viewed as maximum rate:
    * resulting rate can't exceed the output rate of `this` stream.
    */
  def metered[F2[x] >: F[x]: Temporal](rate: FiniteDuration): Stream[F2, O] =
    Stream.fixedRate[F2](rate).zipRight(this)

  /** Provides the same functionality as [[metered]] but begins immediately instead of waiting for `rate`
    */
  def meteredStartImmediately[F2[x] >: F[x]: Temporal](rate: FiniteDuration): Stream[F2, O] =
    (Stream.emit(()) ++ Stream.fixedRate[F2](rate)).zipRight(this)

  /** Logs the elements of this stream as they are pulled.
    *
    * By default, `toString` is called on each element and the result is printed
    * to standard out. To change formatting, supply a value for the `formatter`
    * param. To change the destination, supply a value for the `logger` param.
    *
    * This method does not change the chunk structure of the stream. To debug the
    * chunk structure, see [[debugChunks]].
    *
    * Logging is not done in `F` because this operation is intended for debugging,
    * including pure streams.
    *
    * @example {{{
    * scala> Stream(1, 2).append(Stream(3, 4)).debug(o => s"a: $o").toList
    * a: 1
    * a: 2
    * a: 3
    * a: 4
    * res0: List[Int] = List(1, 2, 3, 4)
    * }}}
    */
  def debug[O2 >: O](
      formatter: O2 => String = (o2: O2) => o2.toString,
      logger: String => Unit = println(_)
  ): Stream[F, O] =
    map { o =>
      logger(formatter(o))
      o
    }

  /** Like [[debug]] but logs chunks as they are pulled instead of individual elements.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).append(Stream(4, 5, 6)).debugChunks(c => s"a: $c").buffer(2).debugChunks(c => s"b: $c").toList
    * a: Chunk(1, 2, 3)
    * b: Chunk(1, 2)
    * a: Chunk(4, 5, 6)
    * b: Chunk(3, 4)
    * b: Chunk(5, 6)
    * res0: List[Int] = List(1, 2, 3, 4, 5, 6)
    * }}}
    */
  def debugChunks[O2 >: O](
      formatter: Chunk[O2] => String = (os: Chunk[O2]) => os.toString,
      logger: String => Unit = println(_)
  ): Stream[F, O] =
    chunks.flatMap { os =>
      logger(formatter(os))
      Stream.chunk(os)
    }

  /** Returns a stream that when run, sleeps for duration `d` and then pulls from this stream.
    *
    * Alias for `sleep_[F](d) ++ this`.
    */
  def delayBy[F2[x] >: F[x]: Temporal](d: FiniteDuration): Stream[F2, O] =
    Stream.sleep_[F2](d) ++ this

  /** Skips the first element that matches the predicate.
    *
    * @example {{{
    * scala> Stream.range(1, 10).delete(_ % 2 == 0).toList
    * res0: List[Int] = List(1, 3, 4, 5, 6, 7, 8, 9)
    * }}}
    */
  def delete(p: O => Boolean): Stream[F, O] =
    this.pull
      .takeWhile(o => !p(o))
      .flatMap {
        case None    => Pull.done
        case Some(s) => s.drop(1).pull.echo
      }
      .stream

  /** Removes all output values from this stream.
    *
    * Often used with `merge` to run one side of the merge for its effect
    * while getting outputs from the opposite side of the merge.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream.eval(SyncIO(println("x"))).drain.compile.toVector.unsafeRunSync()
    * res0: Vector[INothing] = Vector()
    * }}}
    */
  def drain: Stream[F, INothing] =
    this.repeatPull(_.uncons.flatMap(uc => Pull.pure(uc.map(_._2))))

  /** Drops `n` elements of the input, then echoes the rest.
    *
    * @example {{{
    * scala> Stream.range(0,10).drop(5).toList
    * res0: List[Int] = List(5, 6, 7, 8, 9)
    * }}}
    */
  def drop(n: Long): Stream[F, O] =
    this.pull.drop(n).flatMap(_.map(_.pull.echo).getOrElse(Pull.done)).stream

  /** Drops the last element.
    *
    * @example {{{
    * scala> Stream.range(0,10).dropLast.toList
    * res0: List[Int] = List(0, 1, 2, 3, 4, 5, 6, 7, 8)
    * }}}
    */
  def dropLast: Stream[F, O] = dropLastIf(_ => true)

  /** Drops the last element if the predicate evaluates to true.
    *
    * @example {{{
    * scala> Stream.range(0,10).dropLastIf(_ > 5).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4, 5, 6, 7, 8)
    * }}}
    */
  def dropLastIf(p: O => Boolean): Stream[F, O] = {
    def go(last: Chunk[O], s: Stream[F, O]): Pull[F, O, Unit] =
      s.pull.uncons.flatMap {
        case Some((hd, tl)) =>
          Pull.output(last) >> go(hd, tl)
        case None =>
          val o = last(last.size - 1)
          if (p(o)) {
            val (prefix, _) = last.splitAt(last.size - 1)
            Pull.output(prefix)
          } else Pull.output(last)
      }
    this.pull.uncons.flatMap {
      case Some((hd, tl)) => go(hd, tl)
      case None           => Pull.done
    }.stream
  }

  /** Outputs all but the last `n` elements of the input.
    *
    * This is a '''pure''' stream operation: if `s` is a finite pure stream, then `s.dropRight(n).toList`
    * is equal to `this.toList.reverse.drop(n).reverse`.
    *
    * @example {{{
    * scala> Stream.range(0,10).dropRight(5).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4)
    * }}}
    */
  def dropRight(n: Int): Stream[F, O] =
    if (n <= 0) this
    else {
      def go(acc: Chunk[O], s: Stream[F, O]): Pull[F, O, Unit] =
        s.pull.uncons.flatMap {
          case None => Pull.done
          case Some((hd, tl)) =>
            val all = acc ++ hd
            Pull.output(all.dropRight(n)) >> go(all.takeRight(n), tl)
        }
      go(Chunk.empty, this).stream
    }

  /** Like [[dropWhile]], but drops the first value which tests false.
    *
    * @example {{{
    * scala> Stream.range(0,10).dropThrough(_ != 4).toList
    * res0: List[Int] = List(5, 6, 7, 8, 9)
    * }}}
    *
    * '''Pure:''' if `this` is a finite pure stream, then `this.dropThrough(p).toList` is equal to
    *   `this.toList.dropWhile(p).drop(1)`
    */
  def dropThrough(p: O => Boolean): Stream[F, O] =
    this.pull
      .dropThrough(p)
      .flatMap(_.map(_.pull.echo).getOrElse(Pull.done))
      .stream

  /** Drops elements from the head of this stream until the supplied predicate returns false.
    *
    * @example {{{
    * scala> Stream.range(0,10).dropWhile(_ != 4).toList
    * res0: List[Int] = List(4, 5, 6, 7, 8, 9)
    * }}}
    *
    * '''Pure''' this operation maps directly to `List.dropWhile`
    */
  def dropWhile(p: O => Boolean): Stream[F, O] =
    this.pull
      .dropWhile(p)
      .flatMap(_.map(_.pull.echo).getOrElse(Pull.done))
      .stream

  /** Like `[[merge]]`, but tags each output with the branch it came from.
    *
    * @example {{{
    * scala> import scala.concurrent.duration._, cats.effect.IO, cats.effect.unsafe.implicits.global
    * scala> val s1 = Stream.awakeEvery[IO](1000.millis).scan(0)((acc, _) => acc + 1)
    * scala> val s = s1.either(Stream.sleep_[IO](500.millis) ++ s1).take(10)
    * scala> s.take(10).compile.toVector.unsafeRunSync()
    * res0: Vector[Either[Int,Int]] = Vector(Left(0), Right(0), Left(1), Right(1), Left(2), Right(2), Left(3), Right(3), Left(4), Right(4))
    * }}}
    */
  def either[F2[x] >: F[x]: Concurrent, O2](
      that: Stream[F2, O2]
  ): Stream[F2, Either[O, O2]] =
    map(Left(_)).merge(that.map(Right(_)))

  /** Enqueues the elements of this stream to the supplied queue.
    */
  def enqueueUnterminated[F2[x] >: F[x], O2 >: O](queue: QueueSink[F2, O2]): Stream[F2, Nothing] =
    this.foreach(queue.offer)

  private[fs2] def enqueueUnterminated[F2[x] >: F[x], O2 >: O](
      queue: Queue[F2, O2]
  ): Stream[F2, Nothing] =
    enqueueUnterminated(queue: QueueSink[F2, O2])

  /** Enqueues the chunks of this stream to the supplied queue.
    */
  def enqueueUnterminatedChunks[F2[x] >: F[x], O2 >: O](
      queue: QueueSink[F2, Chunk[O2]]
  ): Stream[F2, Nothing] =
    this.chunks.foreach(queue.offer)

  private[fs2] def enqueueUnterminatedChunks[F2[x] >: F[x], O2 >: O](
      queue: Queue[F2, Chunk[O2]]
  ): Stream[F2, Nothing] = enqueueUnterminatedChunks(queue: QueueSink[F2, Chunk[O2]])

  /** Enqueues the elements of this stream to the supplied queue and enqueues `None` when this stream terminates.
    */
  def enqueueNoneTerminated[F2[x] >: F[x], O2 >: O](
      queue: QueueSink[F2, Option[O2]]
  ): Stream[F2, Nothing] =
    this.noneTerminate.foreach(queue.offer)

  private[fs2] def enqueueNoneTerminated[F2[x] >: F[x], O2 >: O](
      queue: Queue[F2, Option[O2]]
  ): Stream[F2, Nothing] = enqueueNoneTerminated(queue: QueueSink[F2, Option[O2]])

  /** Enqueues the chunks of this stream to the supplied queue and enqueues `None` when this stream terminates.
    */
  def enqueueNoneTerminatedChunks[F2[x] >: F[x], O2 >: O](
      queue: QueueSink[F2, Option[Chunk[O2]]]
  ): Stream[F2, Nothing] =
    this.chunks.noneTerminate.foreach(queue.offer)

  private[fs2] def enqueueNoneTerminatedChunks[F2[x] >: F[x], O2 >: O](
      queue: Queue[F2, Option[Chunk[O2]]]
  ): Stream[F2, Nothing] = enqueueNoneTerminatedChunks(queue: QueueSink[F2, Option[Chunk[O2]]])

  /** Alias for `flatMap(o => Stream.eval(f(o)))`.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream(1,2,3,4).evalMap(i => SyncIO(println(i))).compile.drain.unsafeRunSync()
    * res0: Unit = ()
    * }}}
    *
    * Note this operator will de-chunk the stream back into chunks of size 1,
    * which has performance implications. For maximum performance, `evalMapChunk`
    * is available, however, with caveats.
    */
  def evalMap[F2[x] >: F[x], O2](f: O => F2[O2]): Stream[F2, O2] =
    flatMap(o => Stream.eval(f(o)))

  /** Like `evalMap`, but operates on chunks for performance. This means this operator
    * is not lazy on every single element, rather on the chunks.
    *
    * For instance, `evalMap` would only print twice in the follow example (note the `take(2)`):
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream(1,2,3,4).evalMap(i => SyncIO(println(i))).take(2).compile.drain.unsafeRunSync()
    * res0: Unit = ()
    * }}}
    *
    * But with `evalMapChunk`, it will print 4 times:
    * @example {{{
    * scala> Stream(1,2,3,4).evalMapChunk(i => SyncIO(println(i))).take(2).compile.drain.unsafeRunSync()
    * res0: Unit = ()
    * }}}
    */
  def evalMapChunk[F2[x] >: F[x]: Applicative, O2](f: O => F2[O2]): Stream[F2, O2] =
    chunks.flatMap(o => Stream.evalUnChunk(o.traverse(f)))

  /** Like `[[Stream#mapAccumulate]]`, but accepts a function returning an `F[_]`.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream(1,2,3,4).covary[SyncIO].evalMapAccumulate(0)((acc,i) => SyncIO((i, acc + i))).compile.toVector.unsafeRunSync()
    * res0: Vector[(Int, Int)] = Vector((1,1), (2,3), (3,5), (4,7))
    * }}}
    */
  def evalMapAccumulate[F2[x] >: F[x], S, O2](
      s: S
  )(f: (S, O) => F2[(S, O2)]): Stream[F2, (S, O2)] = {
    def go(s: S, in: Stream[F2, O]): Pull[F2, (S, O2), Unit] =
      in.pull.uncons1.flatMap {
        case None => Pull.done
        case Some((hd, tl)) =>
          Pull.eval(f(s, hd)).flatMap { case (ns, o) =>
            Pull.output1((ns, o)) >> go(ns, tl)
          }
      }

    go(s, this).stream
  }

  /** Effectfully maps and filters the elements of the stream depending on the optionality of the result of the
    * application of the effectful function `f`.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO, cats.syntax.all._
    * scala> Stream(1, 2, 3, 4, 5).evalMapFilter(n => SyncIO((n * 2).some.filter(_ % 4 == 0))).compile.toList.unsafeRunSync()
    * res0: List[Int] = List(4, 8)
    * }}}
    */
  def evalMapFilter[F2[x] >: F[x], O2](f: O => F2[Option[O2]]): Stream[F2, O2] =
    evalMap(f).collect { case Some(v) => v }

  /** Like `[[Stream#scan]]`, but accepts a function returning an `F[_]`.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream(1,2,3,4).covary[SyncIO].evalScan(0)((acc,i) => SyncIO(acc + i)).compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(0, 1, 3, 6, 10)
    * }}}
    */
  def evalScan[F2[x] >: F[x], O2](z: O2)(f: (O2, O) => F2[O2]): Stream[F2, O2] = {
    def go(z: O2, s: Stream[F2, O]): Pull[F2, O2, Unit] =
      s.pull.uncons1.flatMap {
        case Some((hd, tl)) =>
          Pull.eval(f(z, hd)).flatMap(o => Pull.output1(o) >> go(o, tl))
        case None => Pull.done
      }
    (Pull.output1(z) >> go(z, this)).stream
  }

  /** Like `observe` but observes with a function `O => F[O2]` instead of a pipe.
    * Not as powerful as `observe` since not all pipes can be represented by `O => F[O2]`, but much faster.
    * Alias for `evalMap(o => f(o).as(o))`.
    */
  def evalTap[F2[x] >: F[x]: Functor, O2](f: O => F2[O2]): Stream[F2, O] =
    evalMap(o => f(o).as(o))

  /** Alias for `evalMapChunk(o => f(o).as(o))`.
    */
  def evalTapChunk[F2[x] >: F[x]: Applicative, O2](f: O => F2[O2]): Stream[F2, O] =
    evalMapChunk(o => f(o).as(o))

  /** Emits `true` as soon as a matching element is received, else `false` if no input matches.
    * '''Pure''': this operation maps to `List.exists`
    *
    * @example {{{
    * scala> Stream.range(0,10).exists(_ == 4).toList
    * res0: List[Boolean] = List(true)
    * scala> Stream.range(0,10).exists(_ == 10).toList
    * res1: List[Boolean] = List(false)
    * }}}
    * @return Either a singleton stream, or a `never` stream.
    *  - If `this` is a finite stream, the result is a singleton stream, with after yielding one single value.
    *    If `this` is empty, that value is the `mempty` of the instance of `Monoid`.
    *  - If `this` is a non-terminating stream, and no matter if it yields any value, then the result is
    *    equivalent to the `Stream.never`: it never terminates nor yields any value.
    */
  def exists(p: O => Boolean): Stream[F, Boolean] =
    this.pull.forall(!p(_)).flatMap(r => Pull.output1(!r)).stream

  /** Emits only inputs which match the supplied predicate.
    *
    * This is a '''pure''' operation, that projects directly into `List.filter`
    *
    * @example {{{
    * scala> Stream.range(0,10).filter(_ % 2 == 0).toList
    * res0: List[Int] = List(0, 2, 4, 6, 8)
    * }}}
    */
  def filter(p: O => Boolean): Stream[F, O] = mapChunks(_.filter(p))

  /** Like `filter`, but allows filtering based on an effect.
    *
    * Note: The result Stream will consist of chunks that are empty or 1-element-long.
    * If you want to operate on chunks after using it, consider buffering, e.g. by using [[buffer]].
    */
  def evalFilter[F2[x] >: F[x]](f: O => F2[Boolean]): Stream[F2, O] =
    flatMap(o => Stream.eval(f(o)).ifM(Stream.emit(o), Stream.empty))

  /** Like `filter`, but allows filtering based on an effect, with up to `maxConcurrent` concurrently running effects.
    * The ordering of emitted elements is unchanged.
    */
  def evalFilterAsync[F2[x] >: F[
    x
  ]: Concurrent](
      maxConcurrent: Int
  )(f: O => F2[Boolean]): Stream[F2, O] =
    parEvalMap[F2, Stream[F2, O]](maxConcurrent) { o =>
      f(o).map(if (_) Stream.emit(o) else Stream.empty)
    }.flatten

  /** Like `filterNot`, but allows filtering based on an effect.
    *
    * Note: The result Stream will consist of chunks that are empty or 1-element-long.
    * If you want to operate on chunks after using it, consider buffering, e.g. by using [[buffer]].
    */
  def evalFilterNot[F2[x] >: F[x]](f: O => F2[Boolean]): Stream[F2, O] =
    flatMap(o => Stream.eval(f(o)).ifM(Stream.empty, Stream.emit(o)))

  /** Like `filterNot`, but allows filtering based on an effect, with up to `maxConcurrent` concurrently running effects.
    * The ordering of emitted elements is unchanged.
    */
  def evalFilterNotAsync[F2[x] >: F[
    x
  ]: Concurrent](
      maxConcurrent: Int
  )(f: O => F2[Boolean]): Stream[F2, O] =
    parEvalMap[F2, Stream[F2, O]](maxConcurrent) { o =>
      f(o).map(if (_) Stream.empty else Stream.emit(o))
    }.flatten

  /** Like `filter`, but the predicate `f` depends on the previously emitted and
    * current elements.
    *
    * @example {{{
    * scala> Stream(1, -1, 2, -2, 3, -3, 4, -4).filterWithPrevious((previous, current) => previous < current).toList
    * res0: List[Int] = List(1, 2, 3, 4)
    * }}}
    */
  def filterWithPrevious(f: (O, O) => Boolean): Stream[F, O] = {
    def go(last: O, s: Stream[F, O]): Pull[F, O, Unit] =
      s.pull.uncons.flatMap {
        case None           => Pull.done
        case Some((hd, tl)) =>
          // Check if we can emit this chunk unmodified
          val (allPass, newLast) = hd.foldLeft((true, last)) { case ((acc, last), o) =>
            (acc && f(last, o), o)
          }
          if (allPass)
            Pull.output(hd) >> go(newLast, tl)
          else {
            val (acc, newLast) = hd.foldLeft((Vector.empty[O], last)) { case ((acc, last), o) =>
              if (f(last, o)) (acc :+ o, o)
              else (acc, last)
            }
            Pull.output(Chunk.vector(acc)) >> go(newLast, tl)
          }
      }
    this.pull.uncons1.flatMap {
      case None           => Pull.done
      case Some((hd, tl)) => Pull.output1(hd) >> go(hd, tl)
    }.stream
  }

  /** Emits the first input (if any) which matches the supplied predicate.
    *
    * @example {{{
    * scala> Stream.range(1,10).find(_ % 2 == 0).toList
    * res0: List[Int] = List(2)
    * }}}
    * '''Pure''' if `s` is a finite pure stream, `s.find(p).toList` is equal to `s.toList.find(p).toList`,
    *  where the second `toList` is to turn `Option` into `List`.
    */
  def find(f: O => Boolean): Stream[F, O] =
    this.pull
      .find(f)
      .flatMap {
        _.map { case (hd, _) => Pull.output1(hd) }.getOrElse(Pull.done)
      }
      .stream

  /** Creates a stream whose elements are generated by applying `f` to each output of
    * the source stream and concatenated all of the results.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).flatMap { i => Stream.chunk(Chunk.seq(List.fill(i)(i))) }.toList
    * res0: List[Int] = List(1, 2, 2, 3, 3, 3)
    * }}}
    */
  @nowarn("cat=unused-params")
  def flatMap[F2[x] >: F[x], O2](
      f: O => Stream[F2, O2]
  )(implicit ev: NotGiven[O <:< Nothing]): Stream[F2, O2] =
    new Stream(Pull.flatMapOutput[F, F2, O, O2](underlying, (o: O) => f(o).underlying))

  /** Alias for `flatMap(_ => s2)`. */
  def >>[F2[x] >: F[x], O2](
      s2: => Stream[F2, O2]
  )(implicit ev: NotGiven[O <:< Nothing]): Stream[F2, O2] =
    flatMap(_ => s2)

  /** Flattens a stream of streams in to a single stream by concatenating each stream.
    * See `parJoin` and `parJoinUnbounded` for concurrent flattening of 'n' streams.
    */
  def flatten[F2[x] >: F[x], O2](implicit ev: O <:< Stream[F2, O2]): Stream[F2, O2] =
    flatMap(i => ev(i))

  /** Folds all inputs using an initial value `z` and supplied binary operator,
    * and emits a single element stream.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4, 5).fold(0)(_ + _).toList
    * res0: List[Int] = List(15)
    * }}}
    */
  def fold[O2](z: O2)(f: (O2, O) => O2): Stream[F, O2] =
    this.pull.fold(z)(f).flatMap(Pull.output1).stream

  /** Folds all inputs using the supplied binary operator, and emits a single-element
    * stream, or the empty stream if the input is empty, or the never stream if the input is non-terminating.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4, 5).fold1(_ + _).toList
    * res0: List[Int] = List(15)
    * }}}
    */
  def fold1[O2 >: O](f: (O2, O2) => O2): Stream[F, O2] =
    this.pull.fold1(f).flatMap(_.map(Pull.output1).getOrElse(Pull.done)).stream

  /** Alias for `map(f).foldMonoid`.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4, 5).foldMap(_ => 1).toList
    * res0: List[Int] = List(5)
    * }}}
    */
  def foldMap[O2](f: O => O2)(implicit O2: Monoid[O2]): Stream[F, O2] =
    fold(O2.empty)((acc, o) => O2.combine(acc, f(o)))

  /** Folds this stream with the monoid for `O`.
    *
    * @return Either a singleton stream or a `never` stream:
    *  - If `this` is a finite stream, the result is a singleton stream.
    *    If `this` is empty, that value is the `mempty` of the instance of `Monoid`.
    *  - If `this` is a non-terminating stream, and no matter if it yields any value, then the result is
    *    equivalent to the `Stream.never`: it never terminates nor yields any value.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4, 5).foldMonoid.toList
    * res0: List[Int] = List(15)
    * }}}
    */
  def foldMonoid[O2 >: O](implicit O: Monoid[O2]): Stream[F, O2] =
    fold(O.empty)(O.combine)

  /** Emits `false` and halts as soon as a non-matching element is received; or
    * emits a single `true` value if it reaches the stream end and every input before that matches the predicate;
    * or hangs without emitting values if the input is infinite and all inputs match the predicate.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4, 5).forall(_ < 10).toList
    * res0: List[Boolean] = List(true)
    * }}}
    * @return Either a singleton or a never stream:
    * - '''If''' `this` yields an element `x` for which `¬ p(x)`, '''then'''
    *   a singleton stream with the value `false`. Pulling from the resultg
    *   performs all the effects needed until reaching the counterexample `x`.
    * - If `this` is a finite stream with no counterexamples of `p`, '''then''' a singleton stream with the `true` value.
    *   Pulling from the it will perform all effects of `this`.
    * - If `this` is an infinite stream and all its the elements satisfy  `p`, then the result
    *   is a `never` stream. Pulling from that stream will pull all effects from `this`.
    */
  def forall(p: O => Boolean): Stream[F, Boolean] =
    this.pull.forall(p).flatMap(Pull.output1).stream

  /** Like `evalMap` but discards the result of evaluation, resulting
    * in a stream with no elements.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream(1,2,3,4).foreach(i => SyncIO(println(i))).compile.drain.unsafeRunSync()
    * res0: Unit = ()
    * }}}
    */
  def foreach[F2[x] >: F[x]](f: O => F2[Unit]): Stream[F2, INothing] =
    flatMap(o => Stream.exec(f(o)))

  /** Partitions the input into a stream of chunks according to a discriminator function.
    *
    * Each chunk in the source stream is grouped using the supplied discriminator function
    * and the results of the grouping are emitted each time the discriminator function changes
    * values.
    *
    * Note: there is no limit to how large a group can become. To limit the group size, use
    * [[groupAdjacentByLimit]].
    *
    * @example {{{
    * scala> Stream("Hello", "Hi", "Greetings", "Hey").groupAdjacentBy(_.head).toList.map { case (k,vs) => k -> vs.toList }
    * res0: List[(Char,List[String])] = List((H,List(Hello, Hi)), (G,List(Greetings)), (H,List(Hey)))
    * }}}
    */
  def groupAdjacentBy[O2](f: O => O2)(implicit eq: Eq[O2]): Stream[F, (O2, Chunk[O])] =
    groupAdjacentByLimit(Int.MaxValue)(f)

  /** Like [[groupAdjacentBy]] but limits the size of emitted chunks.
    *
    * @example {{{
    * scala> Stream.range(0, 12).groupAdjacentByLimit(3)(_ / 4).toList
    * res0: List[(Int,Chunk[Int])] = List((0,Chunk(0, 1, 2)), (0,Chunk(3)), (1,Chunk(4, 5, 6)), (1,Chunk(7)), (2,Chunk(8, 9, 10)), (2,Chunk(11)))
    * }}}
    */
  def groupAdjacentByLimit[O2](
      limit: Int
  )(f: O => O2)(implicit eq: Eq[O2]): Stream[F, (O2, Chunk[O])] = {
    def go(current: Option[(O2, Chunk[O])], s: Stream[F, O]): Pull[F, (O2, Chunk[O]), Unit] =
      s.pull.unconsLimit(limit).flatMap {
        case Some((hd, tl)) =>
          val (k1, out) = current.getOrElse((f(hd(0)), Chunk.empty[O]))
          doChunk(hd, tl, k1, out, collection.immutable.Queue.empty)
        case None =>
          current
            .map { case (k1, out) =>
              if (out.size == 0) Pull.done else Pull.output1((k1, out))
            }
            .getOrElse(Pull.done)
      }

    @tailrec
    def doChunk(
        chunk: Chunk[O],
        s: Stream[F, O],
        k1: O2,
        out: Chunk[O],
        acc: collection.immutable.Queue[(O2, Chunk[O])]
    ): Pull[F, (O2, Chunk[O]), Unit] = {
      val differsAt = chunk.indexWhere(v => eq.neqv(f(v), k1)).getOrElse(-1)
      if (differsAt == -1) {
        // whole chunk matches the current key, add this chunk to the accumulated output
        if (out.size + chunk.size < limit) {
          val newCurrent = Some((k1, out ++ chunk))
          Pull.output(Chunk.seq(acc)) >> go(newCurrent, s)
        } else {
          val (prefix, suffix) = chunk.splitAt(limit - out.size)
          Pull.output(Chunk.seq(acc :+ ((k1, out ++ prefix)))) >> go(
            Some((k1, suffix)),
            s
          )
        }
      } else {
        // at least part of this chunk does not match the current key, need to group and retain chunkiness
        // split the chunk into the bit where the keys match and the bit where they don't
        val matching = chunk.take(differsAt)
        val newAcc = {
          val newOutSize = out.size + matching.size
          if (newOutSize == 0)
            acc
          else if (newOutSize > limit) {
            val (prefix, suffix) = matching.splitAt(limit - out.size)
            acc :+ ((k1, out ++ prefix)) :+ ((k1, suffix))
          } else
            acc :+ ((k1, out ++ matching))
        }
        val nonMatching = chunk.drop(differsAt)
        // nonMatching is guaranteed to be non-empty here, because we know the last element of the chunk doesn't have
        // the same key as the first
        val k2 = f(nonMatching(0))
        doChunk(
          nonMatching,
          s,
          k2,
          Chunk.empty,
          newAcc
        )
      }
    }

    go(None, this).stream
  }

  /** Divides this stream into chunks of elements of size `n`.
    * Each time a group of size `n` is emitted, `timeout` is reset.
    *
    * If the current chunk does not reach size `n` by the time the
    * `timeout` period elapses, it emits a chunk containing however
    * many elements have been accumulated so far, and resets
    * `timeout`.
    *
    * However, if no elements at all have been accumulated when
    * `timeout` expires, empty chunks are *not* emitted, and `timeout`
    * is not reset.
    * Instead, the next chunk to arrive is emitted immediately (since
    * the stream is still in a timed out state), and only then is
    * `timeout` reset. If the chunk received in a timed out state is
    * bigger than `n`, the first `n` elements of it are emitted
    * immediately in a chunk, `timeout` is reset, and the remaining
    * elements are used for the next chunk.
    *
    * When the stream terminates, any accumulated elements are emitted
    * immediately in a chunk, even if `timeout` has not expired.
    */
  def groupWithin[F2[x] >: F[x]](
      n: Int,
      timeout: FiniteDuration
  )(implicit F: Temporal[F2]): Stream[F2, Chunk[O]] = {

    case class JunctionBuffer[T](
        data: Vector[T],
        endOfSupply: Option[Either[Throwable, Unit]],
        endOfDemand: Option[Either[Throwable, Unit]]
    ) {
      def splitAt(n: Int): (JunctionBuffer[T], JunctionBuffer[T]) =
        if (this.data.size >= n) {
          val (head, tail) = this.data.splitAt(n.toInt)
          (this.copy(tail), this.copy(head))
        } else {
          (this.copy(Vector.empty), this)
        }
    }

    fs2.Stream.force {
      for {
        demand <- Semaphore[F2](n.toLong)
        supply <- Semaphore[F2](0L)
        buffer <- Ref[F2].of(
          JunctionBuffer[O](Vector.empty[O], endOfSupply = None, endOfDemand = None)
        )
      } yield {
        def enqueue(t: O): F2[Boolean] =
          for {
            _ <- demand.acquire
            buf <- buffer.modify(buf => (buf.copy(buf.data :+ t), buf))
            _ <- supply.release
          } yield buf.endOfDemand.isEmpty

        def waitN(s: Semaphore[F2]) =
          F.guaranteeCase(s.acquireN(n.toLong)) {
            case Outcome.Succeeded(_) => s.releaseN(n.toLong)
            case _                    => F.unit
          }

        def acquireSupplyUpToNWithin(n: Long): F2[Long] =
          // in JS cancellation doesn't always seem to run, so race conditions should restore state on their own
          F.race(
            F.sleep(timeout),
            waitN(supply)
          ).flatMap {
            case Left(_) =>
              for {
                _ <- supply.acquire
                m <- supply.available
                k = m.min(n - 1)
                b <- supply.tryAcquireN(k)
              } yield if (b) k + 1 else 1
            case Right(_) =>
              supply.acquireN(n) *> F.pure(n)
          }

        def dequeueN(n: Int): F2[Option[Vector[O]]] =
          acquireSupplyUpToNWithin(n.toLong).flatMap { n =>
            buffer
              .modify(_.splitAt(n.toInt))
              .flatMap { buf =>
                demand.releaseN(buf.data.size.toLong).flatMap { _ =>
                  buf.endOfSupply match {
                    case Some(Left(error)) =>
                      F.raiseError(error)
                    case Some(Right(_)) if buf.data.isEmpty =>
                      F.pure(None)
                    case _ =>
                      F.pure(Some(buf.data))
                  }
                }
              }
          }

        def endSupply(result: Either[Throwable, Unit]): F2[Unit] =
          buffer.update(_.copy(endOfSupply = Some(result))) *> supply.releaseN(Int.MaxValue)

        def endDemand(result: Either[Throwable, Unit]): F2[Unit] =
          buffer.update(_.copy(endOfDemand = Some(result))) *> demand.releaseN(Int.MaxValue)

        val enqueueAsync = F.start {
          this
            .evalMap(enqueue)
            .forall(identity)
            .onFinalizeCase {
              case ExitCase.Succeeded  => endSupply(Right(()))
              case ExitCase.Errored(e) => endSupply(Left(e))
              case ExitCase.Canceled   => endSupply(Right(()))
            }
            .compile
            .drain
        }

        fs2.Stream
          .bracketCase(enqueueAsync) { case (upstream, exitCase) =>
            val ending = exitCase match {
              case ExitCase.Succeeded  => Right(())
              case ExitCase.Errored(e) => Left(e)
              case ExitCase.Canceled   => Right(())
            }
            endDemand(ending) *> upstream.cancel
          }
          .flatMap { _ =>
            fs2.Stream
              .eval(dequeueN(n))
              .repeat
              .collectWhile { case Some(data) => Chunk.vector(data) }
          }
      }
    }
  }

  /** If `this` terminates with `Stream.raiseError(e)`, invoke `h(e)`.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream(1, 2, 3).append(Stream.raiseError[SyncIO](new RuntimeException)).handleErrorWith(_ => Stream(0)).compile.toList.unsafeRunSync()
    * res0: List[Int] = List(1, 2, 3, 0)
    * }}}
    */
  def handleErrorWith[F2[x] >: F[x], O2 >: O](h: Throwable => Stream[F2, O2]): Stream[F2, O2] =
    new Stream(Pull.scope(underlying).handleErrorWith(e => h(e).underlying))

  /** Emits the first element of this stream (if non-empty) and then halts.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).head.toList
    * res0: List[Int] = List(1)
    * }}}
    */
  def head: Stream[F, O] = take(1)

  /** Converts a discrete stream to a signal. Returns a single-element stream.
    *
    * Resulting signal is initially `initial`, and is updated with latest value
    * produced by `source`. If the source stream is empty, the resulting signal
    * will always be `initial`.
    */
  def hold[F2[x] >: F[x]: Concurrent, O2 >: O](
      initial: O2
  ): Stream[F2, Signal[F2, O2]] =
    Stream.eval(SignallingRef.of[F2, O2](initial)).flatMap { sig =>
      Stream(sig).concurrently(evalMap(sig.set))
    }

  /** Like [[hold]] but does not require an initial value, and hence all output elements are wrapped in `Some`. */
  def holdOption[F2[x] >: F[x]: Concurrent, O2 >: O]: Stream[F2, Signal[F2, Option[O2]]] =
    map(Some(_): Option[O2]).hold(None)

  /** Like [[hold]] but returns a `Resource` rather than a single element stream.
    */
  def holdResource[F2[x] >: F[x]: Concurrent, O2 >: O](
      initial: O2
  ): Resource[F2, Signal[F2, O2]] =
    Stream
      .eval(SignallingRef.of[F2, O2](initial))
      .flatMap(sig => Stream(sig).concurrently(evalMap(sig.set)))
      .compile
      .resource
      .lastOrError

  /**  Like [[holdResource]] but does not require an initial value,
    *  and hence all output elements are wrapped in `Some`.
    */
  def holdOptionResource[F2[x] >: F[
    x
  ]: Concurrent, O2 >: O]: Resource[F2, Signal[F2, Option[O2]]] =
    map(Some(_): Option[O2]).holdResource(None)

  /** Falls back to the supplied stream if this stream finishes without emitting any elements.
    *  Note: fallback occurs any time stream evaluation finishes without emitting,
    *  even when effects have been evaluated.
    *
    * @example {{{
    * scala> Stream.empty.ifEmpty(Stream(1, 2, 3)).toList
    * res0: List[Int] = List(1, 2, 3)
    * scala> Stream.exec(cats.effect.SyncIO(println("Hello"))).ifEmpty(Stream(1, 2, 3)).compile.toList.unsafeRunSync()
    * res1: List[Int] = List(1, 2, 3)
    * }}}
    */
  def ifEmpty[F2[x] >: F[x], O2 >: O](fallback: => Stream[F2, O2]): Stream[F2, O2] =
    this.pull.uncons.flatMap {
      case Some((hd, tl)) => Pull.output(hd) >> tl.underlying
      case None           => fallback.underlying
    }.stream

  /** Emits the supplied value if this stream finishes without emitting any elements.
    *  Note: fallback occurs any time stream evaluation finishes without emitting,
    *  even when effects have been evaluated.
    *
    * @example {{{
    * scala> Stream.empty.ifEmptyEmit(0).toList
    * res0: List[Int] = List(0)
    * }}}
    */
  def ifEmptyEmit[O2 >: O](o: => O2): Stream[F, O2] =
    ifEmpty(Stream.emit(o))

  /** Deterministically interleaves elements, starting on the left, terminating when the end of either branch is reached naturally.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).interleave(Stream(4, 5, 6, 7)).toList
    * res0: List[Int] = List(1, 4, 2, 5, 3, 6)
    * }}}
    */
  def interleave[F2[x] >: F[x], O2 >: O](that: Stream[F2, O2]): Stream[F2, O2] =
    zip(that).flatMap { case (o1, o2) => Stream(o1, o2) }

  /** Deterministically interleaves elements, starting on the left, terminating when the ends of both branches are reached naturally.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).interleaveAll(Stream(4, 5, 6, 7)).toList
    * res0: List[Int] = List(1, 4, 2, 5, 3, 6, 7)
    * }}}
    */
  def interleaveAll[F2[x] >: F[x], O2 >: O](that: Stream[F2, O2]): Stream[F2, O2] =
    map(Some(_): Option[O2])
      .zipAll(that.map(Some(_): Option[O2]))(None, None)
      .flatMap { case (o1Opt, o2Opt) =>
        Stream(o1Opt.toSeq: _*) ++ Stream(o2Opt.toSeq: _*)
      }

  /** Interrupts this stream after the specified duration has passed.
    */
  def interruptAfter[F2[x] >: F[x]: Temporal](
      duration: FiniteDuration
  ): Stream[F2, O] =
    interruptWhen[F2](Temporal[F2].sleep(duration).attempt)

  /** Ties this stream to the given `haltWhenTrue` stream.
    * The resulting stream performs all the effects and emits all the outputs
    * from `this` stream (the fore), until the moment that the `haltWhenTrue`
    * stream ends, be it by emitting `true`, error, or cancellation.
    *
    * The `haltWhenTrue` stream is compiled and drained, asynchronously in the
    * background, until the moment it emits a value `true` or raises an error.
    * This halts as soon as either branch halts.
    *
    * If the `haltWhenTrue` stream ends by raising an error, the resulting stream
    * rethrows that same error. If the `haltWhenTrue` stream is cancelled, then
    * the resulting stream is interrupted (without cancellation).
    *
    * Consider using the overload that takes a `Signal`, `Deferred` or `F[Either[Throwable, Unit]]`.
    */
  def interruptWhen[F2[x] >: F[x]](
      haltWhenTrue: Stream[F2, Boolean]
  )(implicit F: Concurrent[F2]): Stream[F2, O] = Stream.force {
    for {
      interruptL <- F.deferred[Unit]
      interruptR <- F.deferred[Unit]
      backResult <- F.deferred[Either[Throwable, Unit]]
    } yield {
      val watch = haltWhenTrue.exists(x => x).interruptWhen(interruptR.get.attempt).compile.drain

      val wakeWatch = watch.guaranteeCase { (c: Outcome[F2, Throwable, Unit]) =>
        val r = c match {
          case Outcome.Errored(t)   => Left(t)
          case Outcome.Succeeded(_) => Right(())
          case Outcome.Canceled()   => Right(())
        }
        backResult.complete(r) >> interruptL.complete(()).void

      }

      // fromEither: bring to the fore errors from the back-sleeper.
      val stopWatch = interruptR.complete(()) >> backResult.get.flatMap(F.fromEither)
      val backWatch = Stream.bracket(wakeWatch.start)(_ => stopWatch)

      backWatch >> this.interruptWhen(interruptL.get.attempt)
    }
  }

  /** Alias for `interruptWhen(haltWhenTrue.get)`. */
  def interruptWhen[F2[x] >: F[x]](
      haltWhenTrue: Deferred[F2, Either[Throwable, Unit]]
  ): Stream[F2, O] =
    interruptWhen(haltWhenTrue.get)

  /** Alias for `interruptWhen(haltWhenTrue.discrete)`. */
  def interruptWhen[F2[x] >: F[x]: Concurrent](
      haltWhenTrue: Signal[F2, Boolean]
  ): Stream[F2, O] =
    interruptWhen(haltWhenTrue.discrete)

  /** Interrupts the stream, when `haltOnSignal` finishes its evaluation.
    */
  def interruptWhen[F2[x] >: F[x]](
      haltOnSignal: F2[Either[Throwable, Unit]]
  ): Stream[F2, O] =
    (Pull.interruptWhen(haltOnSignal) >> this.pull.echo).stream.interruptScope

  /** Creates a scope that may be interrupted by calling scope#interrupt.
    */
  def interruptScope: Stream[F, O] =
    new Stream(Pull.interruptScope(underlying))

  /** Emits the specified separator between every pair of elements in the source stream.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4, 5).intersperse(0).toList
    * res0: List[Int] = List(1, 0, 2, 0, 3, 0, 4, 0, 5)
    * }}}
    *
    * This method preserves the Chunking structure of `this` stream.
    */
  def intersperse[O2 >: O](separator: O2): Stream[F, O2] = {
    def doChunk(hd: Chunk[O], isFirst: Boolean): Chunk[O2] = {
      val bldr = Vector.newBuilder[O2]
      bldr.sizeHint(hd.size * 2 + (if (isFirst) 1 else 0))
      val iter = hd.iterator
      if (isFirst)
        bldr += iter.next()
      iter.foreach { o =>
        bldr += separator
        bldr += o
      }
      Chunk.vector(bldr.result())
    }
    def go(str: Stream[F, O]): Pull[F, O2, Unit] =
      str.pull.uncons.flatMap {
        case None           => Pull.done
        case Some((hd, tl)) => Pull.output(doChunk(hd, false)) >> go(tl)
      }
    this.pull.uncons.flatMap {
      case None           => Pull.done
      case Some((hd, tl)) => Pull.output(doChunk(hd, true)) >> go(tl)
    }.stream
  }

  /** Returns the last element of this stream, if non-empty.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).last.toList
    * res0: List[Option[Int]] = List(Some(3))
    * }}}
    */
  def last: Stream[F, Option[O]] =
    this.pull.last.flatMap(Pull.output1).stream

  /** Returns the last element of this stream, if non-empty, otherwise the supplied `fallback` value.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).lastOr(0).toList
    * res0: List[Int] = List(3)
    * scala> Stream.empty.lastOr(0).toList
    * res1: List[Int] = List(0)
    * }}}
    */
  def lastOr[O2 >: O](fallback: => O2): Stream[F, O2] =
    this.pull.last.flatMap {
      case Some(o) => Pull.output1(o)
      case None    => Pull.output1(fallback)
    }.stream

  /** Applies the specified pure function to each input and emits the result.
    *
    * @example {{{
    * scala> Stream("Hello", "World!").map(_.size).toList
    * res0: List[Int] = List(5, 6)
    * }}}
    */
  def map[O2](f: O => O2): Stream[F, O2] =
    Pull.mapOutput(underlying, f).streamNoScope

  /** Maps a running total according to `S` and the input with the function `f`.
    *
    * @example {{{
    * scala> Stream("Hello", "World").mapAccumulate(0)((l, s) => (l + s.length, s.head)).toVector
    * res0: Vector[(Int, Char)] = Vector((5,H), (10,W))
    * }}}
    */
  def mapAccumulate[S, O2](init: S)(f: (S, O) => (S, O2)): Stream[F, (S, O2)] = {
    val f2 = (s: S, o: O) => {
      val (newS, newO) = f(s, o)
      (newS, (newS, newO))
    }
    this.scanChunks(init)((acc, c) => c.mapAccumulate(acc)(f2))
  }

  /** Alias for [[parEvalMap]].
    */
  def mapAsync[F2[x] >: F[
    x
  ]: Concurrent, O2](
      maxConcurrent: Int
  )(f: O => F2[O2]): Stream[F2, O2] =
    parEvalMap[F2, O2](maxConcurrent)(f)

  /** Alias for [[parEvalMapUnordered]].
    */
  def mapAsyncUnordered[F2[x] >: F[
    x
  ]: Concurrent, O2](
      maxConcurrent: Int
  )(f: O => F2[O2]): Stream[F2, O2] =
    map(o => Stream.eval(f(o))).parJoin(maxConcurrent)

  /** Applies the specified pure function to each chunk in this stream.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).append(Stream(4, 5, 6)).mapChunks { c => val ints = c.toArraySlice; for (i <- 0 until ints.values.size) ints.values(i) = 0; ints }.toList
    * res0: List[Int] = List(0, 0, 0, 0, 0, 0)
    * }}}
    */
  def mapChunks[O2](f: Chunk[O] => Chunk[O2]): Stream[F, O2] =
    this.repeatPull {
      _.uncons.flatMap {
        case None           => Pull.pure(None)
        case Some((hd, tl)) => Pull.output(f(hd)).as(Some(tl))
      }
    }

  /** Behaves like the identity function but halts the stream on an error and does not return the error.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> (Stream(1,2,3) ++ Stream.raiseError[SyncIO](new RuntimeException) ++ Stream(4, 5, 6)).mask.compile.toList.unsafeRunSync()
    * res0: List[Int] = List(1, 2, 3)
    * }}}
    */
  def mask: Stream[F, O] = this.handleErrorWith(_ => Stream.empty)

  /** Like [[Stream.flatMap]] but interrupts the inner stream when new elements arrive in the outer stream.
    *
    * The implementation will try to preserve chunks like [[Stream.merge]].
    *
    * Finializers of each inner stream are guaranteed to run before the next inner stream starts.
    *
    * When the outer stream stops gracefully, the currently running inner stream will continue to run.
    *
    * When an inner stream terminates/interrupts, nothing happens until the next element arrives
    * in the outer stream(i.e the outer stream holds the stream open during this time or else the
    * stream terminates)
    *
    * When either the inner or outer stream fails, the entire stream fails and the finalizer of the
    * inner stream runs before the outer one.
    */
  def switchMap[F2[x] >: F[x], O2](
      f: O => Stream[F2, O2]
  )(implicit F: Concurrent[F2]): Stream[F2, O2] = {
    val fstream = for {
      guard <- Semaphore[F2](1)
      haltRef <- F.ref[Option[Deferred[F2, Unit]]](None)
    } yield {

      def runInner(o: O, halt: Deferred[F2, Unit]): Stream[F2, O2] =
        Stream.bracketFull[F2, Unit] { poll =>
          poll(guard.acquire) // guard inner with a semaphore to prevent parallel inner streams
        } {
          case (_, ExitCase.Errored(_)) => F.unit // if there's an error, don't start next stream
          case _                        => guard.release
        } >> f(o).interruptWhen(halt.get.attempt)

      def haltedF(o: O): F2[Stream[F2, O2]] =
        for {
          halt <- F.deferred[Unit]
          prev <- haltRef.getAndSet(halt.some)
          _ <- prev.traverse_(_.complete(())) // interrupt previous one if any
        } yield runInner(o, halt)

      this.evalMap(haltedF).parJoin(2)
    }
    Stream.force(fstream)
  }

  /** Interleaves the two inputs nondeterministically. The output stream
    * halts after BOTH `s1` and `s2` terminate normally, or in the event
    * of an uncaught failure on either `s1` or `s2`. Has the property that
    * `merge(Stream.empty, s) == s` and `merge(raiseError(e), s)` will
    * eventually terminate with `raiseError(e)`, possibly after emitting some
    * elements of `s` first.
    *
    * The implementation always tries to pull one chunk from each side
    * before waiting for it to be consumed by resulting stream.
    * As such, there may be up to two chunks (one from each stream)
    * waiting to be processed while the resulting stream
    * is processing elements.
    *
    * Also note that if either side produces empty chunk,
    * the processing on that side continues,
    * w/o downstream requiring to consume result.
    *
    * If either side does not emit anything (i.e. as result of drain) that side
    * will continue to run even when the resulting stream did not ask for more data.
    *
    * Note that even when this is equivalent to `Stream(this, that).parJoinUnbounded`,
    * this implementation is little more efficient
    *
    * @example {{{
    * scala> import scala.concurrent.duration._, cats.effect.IO, cats.effect.unsafe.implicits.global
    * scala> val s1 = Stream.awakeEvery[IO](500.millis).scan(0)((acc, _) => acc + 1)
    * scala> val s = s1.merge(Stream.sleep_[IO](250.millis) ++ s1)
    * scala> s.take(6).compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(0, 0, 1, 1, 2, 2)
    * }}}
    */
  def merge[F2[x] >: F[x], O2 >: O](
      that: Stream[F2, O2]
  )(implicit F: Concurrent[F2]): Stream[F2, O2] = {
    val fstream: F2[Stream[F2, O2]] = for {
      interrupt <- F.deferred[Unit]
      resultL <- F.deferred[Either[Throwable, Unit]]
      resultR <- F.deferred[Either[Throwable, Unit]]
      otherSideDone <- F.ref[Boolean](false)
      resultChan <- Channel.unbounded[F2, Stream[F2, O2]]
    } yield {

      def watchInterrupted(str: Stream[F2, O2]): Stream[F2, O2] =
        str.interruptWhen(interrupt.get.attempt)

      // action to signal that one stream is finished, and if it is te last one
      // then close te queue (by putting a None in it)
      val doneAndClose: F2[Unit] = otherSideDone.getAndSet(true).flatMap {
        // complete only if other side is done too.
        case true  => resultChan.close.void
        case false => F.unit
      }

      // action to interrupt the processing of both streams by completing interrupt
      // We need to use `attempt` because `interruption` may already be completed.
      val signalInterruption: F2[Unit] = interrupt.complete(()).void

      def go(s: Stream[F2, O2], guard: Semaphore[F2]): Pull[F2, O2, Unit] =
        Pull.eval(guard.acquire) >> s.pull.uncons.flatMap {
          case Some((hd, tl)) =>
            val send = resultChan.send(Stream.chunk(hd).onFinalize(guard.release))
            Pull.eval(send) >> go(tl, guard)
          case None => Pull.done
        }

      def runStream(s: Stream[F2, O2], whenDone: Deferred[F2, Either[Throwable, Unit]]): F2[Unit] =
        // guarantee we process only single chunk at any given time from any given side.
        Semaphore(1).flatMap { guard =>
          val str = watchInterrupted(go(s, guard).stream)
          str.compile.drain.attempt.flatMap {
            // signal completion of our side before we will signal interruption,
            // to make sure our result is always available to others
            case r @ Left(_)  => whenDone.complete(r) >> signalInterruption
            case r @ Right(_) => whenDone.complete(r) >> doneAndClose
          }
        }

      val atRunEnd: F2[Unit] = for {
        _ <- signalInterruption // interrupt so the upstreams have chance to complete
        left <- resultL.get
        right <- resultR.get
        r <- F.fromEither(CompositeFailure.fromResults(left, right))
      } yield r

      val runStreams = runStream(this, resultL).start >> runStream(that, resultR).start

      Stream.bracket(runStreams)(_ => atRunEnd) >> watchInterrupted(resultChan.stream.flatten)
    }
    Stream.eval(fstream).flatten
  }

  /** Like `merge`, but halts as soon as _either_ branch halts. */
  def mergeHaltBoth[F2[x] >: F[x]: Concurrent, O2 >: O](
      that: Stream[F2, O2]
  ): Stream[F2, O2] =
    noneTerminate.merge(that.noneTerminate).unNoneTerminate

  /** Like `merge`, but halts as soon as the `s1` branch halts.
    *
    * Note: it is *not* guaranteed that the last element of the stream will come from `s1`.
    */
  def mergeHaltL[F2[x] >: F[x]: Concurrent, O2 >: O](
      that: Stream[F2, O2]
  ): Stream[F2, O2] =
    noneTerminate.merge(that.map(Some(_))).unNoneTerminate

  /** Like `merge`, but halts as soon as the `s2` branch halts.
    *
    * Note: it is *not* guaranteed that the last element of the stream will come from `s2`.
    */
  def mergeHaltR[F2[x] >: F[x]: Concurrent, O2 >: O](
      that: Stream[F2, O2]
  ): Stream[F2, O2] =
    that.mergeHaltL(this)

  /** Emits each output wrapped in a `Some` and emits a `None` at the end of the stream.
    *
    * `s.noneTerminate.unNoneTerminate == s`
    *
    * @example {{{
    * scala> Stream(1,2,3).noneTerminate.toList
    * res0: List[Option[Int]] = List(Some(1), Some(2), Some(3), None)
    * }}}
    */
  def noneTerminate: Stream[F, Option[O]] = map(Some(_)) ++ Stream.emit(None)

  /** Run `s2` after `this`, regardless of errors during `this`, then reraise any errors encountered during `this`.
    *
    * Note: this should *not* be used for resource cleanup! Use `bracket` or `onFinalize` instead.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).onComplete(Stream(4, 5)).toList
    * res0: List[Int] = List(1, 2, 3, 4, 5)
    * }}}
    */
  def onComplete[F2[x] >: F[x], O2 >: O](s2: => Stream[F2, O2]): Stream[F2, O2] =
    handleErrorWith(e => s2 ++ new Stream(Pull.fail(e))) ++ s2

  /** Runs the supplied effectful action at the end of this stream, regardless of how the stream terminates.
    */
  def onFinalize[F2[x] >: F[x]](f: F2[Unit])(implicit F2: Applicative[F2]): Stream[F2, O] =
    Stream.bracket(F2.unit)(_ => f) >> this

  /** Like [[onFinalize]] but does not introduce a scope, allowing finalization to occur after
    * subsequent appends or other scope-preserving transformations.
    *
    * Scopes can be manually introduced via [[scope]] if desired.
    *
    * Example use case: `a.concurrently(b).onFinalizeWeak(f).compile.resource.use(g)`
    * In this example, use of `onFinalize` would result in `b` shutting down before
    * `g` is run, because `onFinalize` creates a scope, whose lifetime is extended
    * over the compiled resource. By using `onFinalizeWeak` instead, `f` is attached
    * to the scope governing `concurrently`.
    */
  def onFinalizeWeak[F2[x] >: F[x]](f: F2[Unit])(implicit F2: Applicative[F2]): Stream[F2, O] =
    onFinalizeCaseWeak(_ => f)

  /** Like [[onFinalize]] but provides the reason for finalization as an `ExitCase[Throwable]`.
    */
  def onFinalizeCase[F2[x] >: F[x]](
      f: Resource.ExitCase => F2[Unit]
  )(implicit F2: Applicative[F2]): Stream[F2, O] =
    Stream.bracketCase(F2.unit)((_, ec) => f(ec)) >> this

  /** Like [[onFinalizeCase]] but does not introduce a scope, allowing finalization to occur after
    * subsequent appends or other scope-preserving transformations.
    *
    * Scopes can be manually introduced via [[scope]] if desired.
    *
    * See [[onFinalizeWeak]] for more details on semantics.
    */
  def onFinalizeCaseWeak[F2[x] >: F[x]](
      f: Resource.ExitCase => F2[Unit]
  )(implicit F2: Applicative[F2]): Stream[F2, O] =
    new Stream(Pull.acquire[F2, Unit](F2.unit, (_, ec) => f(ec)).flatMap(_ => underlying))

  /** Like [[Stream#evalMap]], but will evaluate effects in parallel, emitting the results
    * downstream in the same order as the input stream. The number of concurrent effects
    * is limited by the `maxConcurrent` parameter.
    *
    * See [[Stream#parEvalMapUnordered]] if there is no requirement to retain the order of
    * the original stream.
    *
    * @example {{{
    * scala> import cats.effect.IO, cats.effect.unsafe.implicits.global
    * scala> Stream(1,2,3,4).covary[IO].parEvalMap(2)(i => IO(println(i))).compile.drain.unsafeRunSync()
    * res0: Unit = ()
    * }}}
    */
  def parEvalMap[F2[x] >: F[x], O2](
      maxConcurrent: Int
  )(f: O => F2[O2])(implicit F: Concurrent[F2]): Stream[F2, O2] = {
    val fstream: F2[Stream[F2, O2]] = for {
      chan <- Channel.bounded[F2, F2[Either[Throwable, O2]]](maxConcurrent)
      chanReadDone <- F.deferred[Unit]
    } yield {
      def forkOnElem(o: O): F2[Stream[F2, Unit]] =
        for {
          value <- F.deferred[Either[Throwable, O2]]
          send = chan.send(value.get).as {
            Stream.eval(f(o).attempt.flatMap(value.complete(_).void))
          }
          eit <- chanReadDone.get.race(send)
        } yield eit match {
          case Left(())      => Stream.empty
          case Right(stream) => stream
        }

      val background = this
        .evalMap(forkOnElem)
        .parJoin(maxConcurrent)
        .onFinalize(chanReadDone.get.race(chan.close).void)

      val foreground =
        chan.stream
          .evalMap(identity)
          .rethrow
          .onFinalize(chanReadDone.complete(()).void)

      foreground.concurrently(background)
    }
    Stream.eval(fstream).flatten
  }

  def prevParEvalMapUnordered[F2[x] >: F[x]: Concurrent, O2](
      maxConcurrent: Int
  )(f: O => F2[O2]): Stream[F2, O2] =
    map(o => Stream.eval(f(o))).parJoin(maxConcurrent)

  /** Like [[Stream#evalMap]], but will evaluate effects in parallel, emitting the results
    * downstream. The number of concurrent effects is limited by the `maxConcurrent` parameter.
    *
    * See [[Stream#parEvalMap]] if retaining the original order of the stream is required.
    *
    * @example {{{
    * scala> import cats.effect.IO, cats.effect.unsafe.implicits.global
    * scala> Stream(1,2,3,4).covary[IO].parEvalMapUnordered(2)(i => IO(println(i))).compile.drain.unsafeRunSync()
    * res0: Unit = ()
    * }}}
    */
  def parEvalMapUnordered[F2[x] >: F[x]: Concurrent, O2](
      maxConcurrent: Int
  )(f: O => F2[O2]): Stream[F2, O2] = {
    assert(maxConcurrent > 0, "maxConcurrent must be > 0, was: " + maxConcurrent)

    // One is taken by inner stream read.
    val concurrency = if (maxConcurrent == Int.MaxValue) Int.MaxValue else maxConcurrent + 1
    val action =
      (
        Semaphore[F2](concurrency),
        Queue.bounded[F2, Option[O2]](concurrency),
        Ref[F2].of(none[Either[NonEmptyList[Throwable], Unit]]),
        Deferred[F2, Either[Throwable, Unit]]
      ).mapN { (semaphore, queue, result, stopReading) =>
        val releaseAndCheckCompletion =
          semaphore.release *>
            semaphore.available
              .product(result.get)
              .flatMap { case (available, completion) =>
                queue.offer(none).whenA(completion.nonEmpty && available == concurrency)
              }

        val succeed =
          result.update {
            case None  => ().asRight.some
            case other => other
          }

        def failed(ex: Throwable) =
          stopReading.complete(().asRight) *>
            result.update {
              case Some(Left(nel)) => nel.prepend(ex).asLeft.some
              case _               => NonEmptyList.one(ex).asLeft.some
            }

        val completeStream =
          Stream.force {
            result.get.map {
              case Some(Left(nel)) => Stream.raiseError[F2](CompositeFailure.fromNel(nel))
              case _               => Stream.empty
            }
          }

        val pullExecAndOutput =
          Stream.exec(semaphore.acquire) ++
            interruptWhen(stopReading)
              .evalMap { el =>
                val running =
                  f(el).attempt
                    .race(stopReading.get)
                    .flatMap {
                      case Left(Left(ex)) => failed(ex)
                      case Left(Right(a)) => queue.offer(a.some)
                      case Right(_)       => ().pure[F2]
                    }
                    .guarantee(releaseAndCheckCompletion)
                    .start
                    .void

                semaphore.acquire *> running
              }
              .onFinalizeCase {
                case ExitCase.Succeeded   => succeed *> releaseAndCheckCompletion
                case ExitCase.Errored(ex) => failed(ex) *> releaseAndCheckCompletion
                case ExitCase.Canceled    => ().pure[F2]
              }

        Stream.fromQueueNoneTerminated(queue).concurrently(pullExecAndOutput) ++ completeStream
      }

    Stream.force(action)
  }

  /** Concurrent zip.
    *
    * It combines elements pairwise and in order like `zip`, but
    * instead of pulling from the left stream and then from the right
    * stream, it evaluates both pulls concurrently.
    * The resulting stream terminates when either stream terminates.
    *
    * The concurrency is bounded following a model of successive
    * races: both sides start evaluation of a single element
    * concurrently, and whichever finishes first waits for the other
    * to catch up and the resulting pair to be emitted, at which point
    * the process repeats. This means that no branch is allowed to get
    * ahead by more than one element.
    *
    * Notes:
    * - Effects within each stream are executed in order, they are
    *   only concurrent with respect to each other.
    * - The output of `parZip` is guaranteed to be the same as `zip`,
    *   although the order in which effects are executed differs.
    */
  def parZip[F2[x] >: F[x]: Concurrent, O2](
      that: Stream[F2, O2]
  ): Stream[F2, (O, O2)] =
    Stream.parZip(this, that)

  /** Like `parZip`, but combines elements pairwise with a function instead
    * of tupling them.
    */
  def parZipWith[F2[x] >: F[x]: Concurrent, O2 >: O, O3, O4](
      that: Stream[F2, O3]
  )(f: (O2, O3) => O4): Stream[F2, O4] =
    this.parZip(that).map(f.tupled)

  /** Pause this stream when `pauseWhenTrue` emits `true`, resuming when `false` is emitted. */
  def pauseWhen[F2[x] >: F[x]: Concurrent](
      pauseWhenTrue: Stream[F2, Boolean]
  ): Stream[F2, O] =
    Stream.eval(SignallingRef[F2, Boolean](false)).flatMap { pauseSignal =>
      def writer = pauseWhenTrue.evalMap(pauseSignal.set).drain

      pauseWhen(pauseSignal).mergeHaltBoth(writer)
    }

  /** Pause this stream when `pauseWhenTrue` is `true`, resume when it's `false`. */
  def pauseWhen[F2[x] >: F[x]: Concurrent](
      pauseWhenTrue: Signal[F2, Boolean]
  ): Stream[F2, O] = {

    def waitToResume =
      pauseWhenTrue.discrete.dropWhile(_ == true).take(1).compile.drain

    def pauseIfNeeded = Stream.exec {
      pauseWhenTrue.get.flatMap(paused => waitToResume.whenA(paused))
    }

    pauseIfNeeded ++ chunks.flatMap { chunk =>
      pauseIfNeeded ++ Stream.chunk(chunk)
    }
  }

  /** Alias for `prefetchN(1)`. */
  def prefetch[F2[x] >: F[x]: Concurrent]: Stream[F2, O] =
    prefetchN[F2](1)

  /** Behaves like `identity`, but starts fetches up to `n` chunks in parallel with downstream
    * consumption, enabling processing on either side of the `prefetchN` to run in parallel.
    */
  def prefetchN[F2[x] >: F[x]: Concurrent](
      n: Int
  ): Stream[F2, O] =
    Stream.eval(Channel.bounded[F2, Chunk[O]](n)).flatMap { chan =>
      chan.stream.flatMap(Stream.chunk).concurrently {
        chunks.through(chan.sendAll)

      }
    }

  /** Prints each element of this stream to standard out, converting each element to a `String` via `Show`. */
  def printlns[F2[x] >: F[x], O2 >: O](implicit
      F: Console[F2],
      showO: Show[O2] = Show.fromToString[O2]
  ): Stream[F2, INothing] =
    foreach((o: O2) => F.println(o.show))

  /** Rechunks the stream such that output chunks are within `[inputChunk.size * minFactor, inputChunk.size * maxFactor]`.
    * The pseudo random generator is deterministic based on the supplied seed.
    */
  def rechunkRandomlyWithSeed[F2[x] >: F[x]](minFactor: Double, maxFactor: Double)(
      seed: Long
  ): Stream[F2, O] =
    Stream.suspend {
      assert(maxFactor >= minFactor, "maxFactor should be greater or equal to minFactor")
      val random = new scala.util.Random(seed)
      def factor: Double = Math.abs(random.nextInt()) % (maxFactor - minFactor) + minFactor

      def go(acc: Chunk[O], size: Option[Int], s: Stream[F2, Chunk[O]]): Pull[F2, O, Unit] = {
        def nextSize(chunk: Chunk[O]): Pull[F2, INothing, Int] =
          size match {
            case Some(size) => Pull.pure(size)
            case None       => Pull.pure((factor * chunk.size).toInt)
          }

        s.pull.uncons1.flatMap {
          case Some((hd, tl)) =>
            nextSize(hd).flatMap { size =>
              if (acc.size < size) go(acc ++ hd, size.some, tl)
              else if (acc.size == size)
                Pull.output(acc) >> go(hd, size.some, tl)
              else {
                val (out, rem) = acc.splitAt(size - 1)
                Pull.output(out) >> go(rem ++ hd, None, tl)
              }
            }
          case None =>
            Pull.output(acc)
        }
      }

      go(Chunk.empty, None, chunks).stream
    }

  /** Rechunks the stream such that output chunks are within [inputChunk.size * minFactor, inputChunk.size * maxFactor].
    */
  def rechunkRandomly[F2[x] >: F[x]](
      minFactor: Double = 0.1,
      maxFactor: Double = 2.0
  ): Stream[F2, O] =
    Stream.suspend(this.rechunkRandomlyWithSeed[F2](minFactor, maxFactor)(System.nanoTime()))

  /** Alias for [[fold1]]. */
  def reduce[O2 >: O](f: (O2, O2) => O2): Stream[F, O2] = fold1(f)

  /** Reduces this stream with the Semigroup for `O`.
    *
    * @example {{{
    * scala> Stream("The", "quick", "brown", "fox").intersperse(" ").reduceSemigroup.toList
    * res0: List[String] = List(The quick brown fox)
    * }}}
    */
  def reduceSemigroup[O2 >: O](implicit S: Semigroup[O2]): Stream[F, O2] =
    reduce[O2](S.combine(_, _))

  /** Repartitions the input with the function `f`. On each step `f` is applied
    * to the input and all elements but the last of the resulting sequence
    * are emitted. The last element is then appended to the next input using the
    * Semigroup `S`.
    *
    * @example {{{
    * scala> Stream("Hel", "l", "o Wor", "ld").repartition(s => Chunk.array(s.split(" "))).toList
    * res0: List[String] = List(Hello, World)
    * }}}
    */
  def repartition[O2 >: O](f: O2 => Chunk[O2])(implicit S: Semigroup[O2]): Stream[F, O2] =
    this.pull
      .scanChunks(Option.empty[O2]) { (carry, chunk) =>
        val (out, (_, c2)) = chunk
          .scanLeftCarry((Chunk.empty[O2], carry)) { case ((_, carry), o) =>
            val o2: O2 = carry.fold(o: O2)(S.combine(_, o))
            val partitions: Chunk[O2] = f(o2)
            if (partitions.isEmpty) partitions -> None
            else if (partitions.size == 1) Chunk.empty -> partitions.last
            else
              partitions.take(partitions.size - 1) -> partitions.last
          }
        (c2, out.flatMap { case (o, _) => o })
      }
      .flatMap {
        case Some(carry) => Pull.output1(carry)
        case None        => Pull.done
      }
      .stream

  /** Repeat this stream an infinite number of times.
    *
    * `s.repeat == s ++ s ++ s ++ ...`
    *
    * @example {{{
    * scala> Stream(1,2,3).repeat.take(8).toList
    * res0: List[Int] = List(1, 2, 3, 1, 2, 3, 1, 2)
    * }}}
    */
  def repeat: Stream[F, O] =
    this ++ repeat

  /** Repeat this stream a given number of times.
    *
    * `s.repeatN(n) == s ++ s ++ s ++ ... (n times)`
    *
    * @example {{{
    * scala> Stream(1,2,3).repeatN(3).take(100).toList
    * res0: List[Int] = List(1, 2, 3, 1, 2, 3, 1, 2, 3)
    * }}}
    */
  def repeatN(n: Long): Stream[F, O] = {
    require(n > 0, "n must be > 0") // same behaviour as sliding
    if (n > 1) this ++ repeatN(n - 1)
    else this
  }

  /** Converts a `Stream[F,Either[Throwable,O]]` to a `Stream[F,O]`, which emits right values and fails upon the first `Left(t)`.
    * Preserves chunkiness.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream(Right(1), Right(2), Left(new RuntimeException), Right(3)).rethrow[SyncIO, Int].handleErrorWith(_ => Stream(-1)).compile.toList.unsafeRunSync()
    * res0: List[Int] = List(-1)
    * }}}
    */
  def rethrow[F2[x] >: F[x], O2](implicit
      ev: O <:< Either[Throwable, O2],
      rt: RaiseThrowable[F2]
  ): Stream[F2, O2] =
    this.asInstanceOf[Stream[F, Either[Throwable, O2]]].chunks.flatMap { c =>
      val firstError = c.collectFirst { case Left(err) => err }
      firstError match {
        case None    => Stream.chunk(c.collect { case Right(i) => i })
        case Some(h) => Stream.raiseError[F2](h)
      }
    }

  /** Left fold which outputs all intermediate results.
    *
    * @example {{{
    * scala> Stream(1,2,3,4).scan(0)(_ + _).toList
    * res0: List[Int] = List(0, 1, 3, 6, 10)
    * }}}
    *
    * More generally:
    *   `Stream().scan(z)(f) == Stream(z)`
    *   `Stream(x1).scan(z)(f) == Stream(z, f(z,x1))`
    *   `Stream(x1,x2).scan(z)(f) == Stream(z, f(z,x1), f(f(z,x1),x2))`
    *   etc
    */
  def scan[O2](z: O2)(f: (O2, O) => O2): Stream[F, O2] =
    (Pull.output1(z) >> scan_(z)(f)).stream

  private def scan_[O2](z: O2)(f: (O2, O) => O2): Pull[F, O2, Unit] =
    this.pull.uncons.flatMap {
      case None => Pull.done
      case Some((hd, tl)) =>
        val (out, carry) = hd.scanLeftCarry(z)(f)
        Pull.output(out) >> tl.scan_(carry)(f)
    }

  /** Like `[[scan]]`, but uses the first element of the stream as the seed.
    *
    * @example {{{
    * scala> Stream(1,2,3,4).scan1(_ + _).toList
    * res0: List[Int] = List(1, 3, 6, 10)
    * }}}
    */
  def scan1[O2 >: O](f: (O2, O2) => O2): Stream[F, O2] =
    this.pull.uncons.flatMap {
      case None => Pull.done
      case Some((hd, tl)) =>
        val (pre, post) = hd.splitAt(1)
        Pull.output(pre) >> tl.cons(post).scan_(pre(0): O2)(f)
    }.stream

  /** Like `scan` but `f` is applied to each chunk of the source stream.
    * The resulting chunk is emitted while the resulting state is used in the
    * next invocation of `f`.
    *
    * Many stateful pipes can be implemented efficiently (i.e., supporting fusion) with this method.
    */
  def scanChunks[S, O2 >: O, O3](init: S)(f: (S, Chunk[O2]) => (S, Chunk[O3])): Stream[F, O3] =
    scanChunksOpt(init)(s => Some(c => f(s, c)))

  /** More general version of `scanChunks` where the current state (i.e., `S`) can be inspected
    * to determine if another chunk should be pulled or if the stream should terminate.
    * Termination is signaled by returning `None` from `f`. Otherwise, a function which consumes
    * the next chunk is returned wrapped in `Some`.
    *
    * @example {{{
    * scala> def take[F[_],O](s: Stream[F,O], n: Int): Stream[F,O] =
    *      |   s.scanChunksOpt(n) { n => if (n <= 0) None else Some((c: Chunk[O]) => if (c.size < n) (n - c.size, c) else (0, c.take(n))) }
    * scala> take(Stream.range(0,100), 5).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4)
    * }}}
    */
  def scanChunksOpt[S, O2 >: O, O3](
      init: S
  )(f: S => Option[Chunk[O2] => (S, Chunk[O3])]): Stream[F, O3] =
    this.pull.scanChunksOpt(init)(f).void.stream

  /** Alias for `map(f).scanMonoid`.
    *
    * @example {{{
    * scala> Stream("a", "aa", "aaa", "aaaa").scanMap(_.length).toList
    * res0: List[Int] = List(0, 1, 3, 6, 10)
    * }}}
    */
  def scanMap[O2](f: O => O2)(implicit O2: Monoid[O2]): Stream[F, O2] =
    scan(O2.empty)((acc, el) => acc |+| f(el))

  /** Folds this stream with the monoid for `O` while emitting all intermediate results.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4).scanMonoid.toList
    * res0: List[Int] = List(0, 1, 3, 6, 10)
    * }}}
    */
  def scanMonoid[O2 >: O](implicit O: Monoid[O2]): Stream[F, O2] =
    scan(O.empty)(O.combine)

  /** Introduces an explicit scope.
    *
    * Scopes are normally introduced automatically, when using `bracket` or similar
    * operations that acquire resources and run finalizers. Manual scope introduction
    * is useful when using [[onFinalizeWeak]]/[[onFinalizeCaseWeak]], where no scope
    * is introduced.
    */
  def scope: Stream[F, O] =
    new Stream(Pull.scope(underlying))

  /** Groups inputs in fixed size chunks by passing a "sliding window"
    * of size `n` over them. If the input contains less than or equal to
    * `n` elements, only one chunk of this size will be emitted.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4).sliding(2).toList
    * res0: List[fs2.Chunk[Int]] = List(Chunk(1, 2), Chunk(2, 3), Chunk(3, 4))
    * }}}
    * @throws scala.IllegalArgumentException if `n` <= 0
    */
  def sliding(n: Int): Stream[F, Chunk[O]] = sliding(n, 1)

  /** Groups inputs in fixed size chunks by passing a "sliding window"
    * of size with step over them. If the input contains less than or equal to
    * `size` elements, only one chunk of this size will be emitted.
    *
    * @example {{{
    * scala> Stream(1, 2, 3, 4, 5).sliding(2, 3).toList
    * res0: List[fs2.Chunk[Int]] = List(Chunk(1, 2), Chunk(4, 5))
    * scala> Stream(1, 2, 3, 4, 5).sliding(3, 2).toList
    * res1: List[fs2.Chunk[Int]] = List(Chunk(1, 2, 3), Chunk(3, 4, 5))
    * }}}
    * @throws scala.IllegalArgumentException if `size` <= 0 | `step` <= 0
    */
  def sliding(size: Int, step: Int): Stream[F, Chunk[O]] = {
    require(size > 0, "size must be > 0")
    require(step > 0, "step must be > 0")

    def stepNotSmallerThanSize(s: Stream[F, O], prev: Chunk[O]): Pull[F, Chunk[O], Unit] =
      s.pull.uncons
        .flatMap {
          case None =>
            if (prev.isEmpty) Pull.done
            else Pull.output1(prev.take(size))
          case Some((hd, tl)) =>
            val buffer = ArrayBuffer.empty[Chunk[O]]
            var current = prev ++ hd
            while (current.size >= step) {
              val (nHeads, nTails) = current.splitAt(step)
              buffer += nHeads.take(size)
              current = nTails
            }
            Pull.output(Chunk.buffer(buffer)) >> stepNotSmallerThanSize(tl, current)
        }

    def stepSmallerThanSize(
        s: Stream[F, O],
        window: Chunk[O],
        prev: Chunk[O]
    ): Pull[F, Chunk[O], Unit] =
      s.pull.uncons
        .flatMap {
          case None =>
            if (prev.isEmpty) Pull.done
            else Pull.output1((window ++ prev).take(size))
          case Some((hd, tl)) =>
            val buffer = ArrayBuffer.empty[Chunk[O]]
            var w = window
            var current = prev ++ hd
            while (current.size >= step) {
              val (head, tail) = current.splitAt(step)
              val wind = w ++ head
              buffer += wind
              w = wind.drop(step)
              current = tail
            }

            Pull.output(Chunk.buffer(buffer)) >> stepSmallerThanSize(tl, w, current)
        }

    val resultPull =
      if (step < size)
        this.pull
          .unconsN(size, true)
          .flatMap {
            case None => Pull.done
            case Some((hd, tl)) =>
              Pull.output1(hd) >> stepSmallerThanSize(tl, hd.drop(step), Chunk.Queue.empty)
          }
      else stepNotSmallerThanSize(this, Chunk.Queue.empty)

    resultPull.stream
  }

  /** Starts this stream and cancels it as finalization of the returned stream.
    */
  def spawn[F2[x] >: F[x]: Concurrent]: Stream[F2, Fiber[F2, Throwable, Unit]] =
    Stream.supervise(this.covary[F2].compile.drain)

  /** Breaks the input into chunks where the delimiter matches the predicate.
    * The delimiter does not appear in the output. Two adjacent delimiters in the
    * input result in an empty chunk in the output.
    *
    * @example {{{
    * scala> Stream.range(0, 10).split(_ % 4 == 0).toList
    * res0: List[Chunk[Int]] = List(empty, Chunk(1, 2, 3), Chunk(5, 6, 7), Chunk(9))
    * }}}
    */
  def split(f: O => Boolean): Stream[F, Chunk[O]] = {
    def go(buffer: Chunk[O], s: Stream[F, O]): Pull[F, Chunk[O], Unit] =
      s.pull.uncons.flatMap {
        case Some((hd, tl)) =>
          hd.indexWhere(f) match {
            case None => go(buffer ++ hd, tl)
            case Some(idx) =>
              val pfx = hd.take(idx)
              val b2 = buffer ++ pfx
              Pull.output1(b2) >> go(Chunk.empty, tl.cons(hd.drop(idx + 1)))
          }
        case None =>
          if (buffer.nonEmpty) Pull.output1(buffer)
          else Pull.done
      }
    go(Chunk.empty, this).stream
  }

  /** Emits all elements of the input except the first one.
    *
    * @example {{{
    * scala> Stream(1,2,3).tail.toList
    * res0: List[Int] = List(2, 3)
    * }}}
    */
  def tail: Stream[F, O] = drop(1)

  /** Emits the first `n` elements of this stream.
    *
    * @example {{{
    * scala> Stream.range(0,1000).take(5).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4)
    * }}}
    */
  def take(n: Long): Stream[F, O] = this.pull.take(n).void.stream

  /** Emits the last `n` elements of the input.
    *
    * @example {{{
    * scala> Stream.range(0,1000).takeRight(5).toList
    * res0: List[Int] = List(995, 996, 997, 998, 999)
    * }}}
    */
  def takeRight(n: Int): Stream[F, O] =
    this.pull
      .takeRight(n)
      .flatMap(Pull.output)
      .stream

  /** Like [[takeWhile]], but emits the first value which tests false.
    *
    * @example {{{
    * scala> Stream.range(0,1000).takeThrough(_ != 5).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4, 5)
    * }}}
    */
  def takeThrough(p: O => Boolean): Stream[F, O] =
    this.pull.takeThrough(p).void.stream

  /** Emits the longest prefix of the input for which all elements test true according to `f`.
    *
    * @example {{{
    * scala> Stream.range(0,1000).takeWhile(_ != 5).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4)
    * }}}
    */
  def takeWhile(p: O => Boolean, takeFailure: Boolean = false): Stream[F, O] =
    this.pull.takeWhile(p, takeFailure).void.stream

  /** Transforms this stream using the given `Pipe`.
    *
    * @example {{{
    * scala> Stream("Hello", "world").through(text.utf8.encode).toVector.toArray
    * res0: Array[Byte] = Array(72, 101, 108, 108, 111, 119, 111, 114, 108, 100)
    * }}}
    */
  def through[F2[x] >: F[x], O2](f: Stream[F, O] => Stream[F2, O2]): Stream[F2, O2] = f(this)

  /** Transforms this stream and `s2` using the given `Pipe2`. */
  def through2[F2[x] >: F[x], O2, O3](
      s2: Stream[F2, O2]
  )(f: (Stream[F, O], Stream[F2, O2]) => Stream[F2, O3]): Stream[F2, O3] =
    f(this, s2)

  /** Fails this stream with a `TimeoutException` if it does not complete within given `timeout`. */
  def timeout[F2[x] >: F[x]: Temporal](
      timeout: FiniteDuration
  ): Stream[F2, O] =
    this.interruptWhen(
      Temporal[F2]
        .sleep(timeout)
        .as(Left(new TimeoutException(s"Timed out after $timeout")))
        .widen[Either[Throwable, Unit]]
    )

  /** Translates effect type from `F` to `G` using the supplied `FunctionK`.
    */
  def translate[F2[x] >: F[x], G[_]](u: F2 ~> G): Stream[G, O] =
    new Stream(Pull.translate[F2, G, O](underlying, u))

  /** Translates effect type from `F` to `G` using the supplied `FunctionK`.
    */
  @deprecated("Use translate instead", "3.0")
  def translateInterruptible[F2[x] >: F[x], G[_]](u: F2 ~> G): Stream[G, O] =
    new Stream(Pull.translate[F2, G, O](underlying, u))

  /** Converts the input to a stream of 1-element chunks.
    *
    * @example {{{
    * scala> (Stream(1,2,3) ++ Stream(4,5,6)).unchunk.chunks.toList
    * res0: List[Chunk[Int]] = List(Chunk(1), Chunk(2), Chunk(3), Chunk(4), Chunk(5), Chunk(6))
    * }}}
    */
  def unchunk: Stream[F, O] =
    this.repeatPull {
      _.uncons1.flatMap {
        case None           => Pull.pure(None)
        case Some((hd, tl)) => Pull.output1(hd).as(Some(tl))
      }
    }

  /** Alias for [[filter]]
    * Implemented to enable filtering in for comprehensions
    */
  def withFilter(f: O => Boolean) = this.filter(f)

  private type ZipWithLeft[G[_], I, O2] = (Chunk[I], Stream[G, I]) => Pull[G, O2, Unit]

  private def zipWith_[F2[x] >: F[x], O2 >: O, O3, O4](that: Stream[F2, O3])(
      k1: ZipWithLeft[F2, O2, O4],
      k2: ZipWithLeft[F2, O3, O4],
      k3: Stream[F2, O3] => Pull[F2, O4, Unit]
  )(f: (O2, O3) => O4): Stream[F2, O4] = {
    def go(
        leg1: Stream.StepLeg[F2, O2],
        leg2: Stream.StepLeg[F2, O3]
    ): Pull[F2, O4, Unit] = {
      val l1h = leg1.head
      val l2h = leg2.head
      val out = l1h.zipWith(l2h)(f)
      Pull.output(out) >> {
        if (l1h.size > l2h.size) {
          val extra1 = l1h.drop(l2h.size)
          leg2.stepLeg.flatMap {
            case None      => k1(extra1, leg1.stream)
            case Some(tl2) => go(leg1.setHead(extra1), tl2)
          }
        } else {
          val extra2 = l2h.drop(l1h.size)
          leg1.stepLeg.flatMap {
            case None      => k2(extra2, leg2.stream)
            case Some(tl1) => go(tl1, leg2.setHead(extra2))
          }
        }
      }
    }

    covaryAll[F2, O2].pull.stepLeg.flatMap {
      case Some(leg1) =>
        that.pull.stepLeg
          .flatMap {
            case Some(leg2) => go(leg1, leg2)
            case None       => k1(leg1.head, leg1.stream)
          }

      case None => k3(that)
    }.stream
  }

  /** Determinsitically zips elements, terminating when the ends of both branches
    * are reached naturally, padding the left branch with `pad1` and padding the right branch
    * with `pad2` as necessary.
    *
    * @example {{{
    * scala> Stream(1,2,3).zipAll(Stream(4,5,6,7))(0,0).toList
    * res0: List[(Int,Int)] = List((1,4), (2,5), (3,6), (0,7))
    * }}}
    */
  def zipAll[F2[x] >: F[x], O2 >: O, O3](
      that: Stream[F2, O3]
  )(pad1: O2, pad2: O3): Stream[F2, (O2, O3)] =
    zipAllWith[F2, O2, O3, (O2, O3)](that)(pad1, pad2)(Tuple2.apply)

  /** Determinsitically zips elements with the specified function, terminating
    * when the ends of both branches are reached naturally, padding the left
    * branch with `pad1` and padding the right branch with `pad2` as necessary.
    *
    * @example {{{
    * scala> Stream(1,2,3).zipAllWith(Stream(4,5,6,7))(0, 0)(_ + _).toList
    * res0: List[Int] = List(5, 7, 9, 7)
    * }}}
    */
  def zipAllWith[F2[x] >: F[x], O2 >: O, O3, O4](
      that: Stream[F2, O3]
  )(pad1: O2, pad2: O3)(f: (O2, O3) => O4): Stream[F2, O4] = {
    def cont1(hd: Chunk[O2], tl: Stream[F2, O2]): Pull[F2, O4, Unit] =
      Pull.output(hd.map(o1 => f(o1, pad2))) >> contLeft(tl)

    def cont2(hd: Chunk[O3], tl: Stream[F2, O3]): Pull[F2, O4, Unit] =
      Pull.output(hd.map(o2 => f(pad1, o2))) >> contRight(tl)

    def contLeft(s: Stream[F2, O2]): Pull[F2, O4, Unit] =
      Pull.mapOutput(s.pull.echo, f(_, pad2))

    def contRight(s: Stream[F2, O3]): Pull[F2, O4, Unit] =
      Pull.mapOutput(s.pull.echo, f(pad1, _))

    zipWith_[F2, O2, O3, O4](that)(cont1, cont2, contRight)(f)
  }

  /** Determinsitically zips elements, terminating when the end of either branch is reached naturally.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).zip(Stream(4, 5, 6, 7)).toList
    * res0: List[(Int,Int)] = List((1,4), (2,5), (3,6))
    * }}}
    */
  def zip[F2[x] >: F[x], O2](that: Stream[F2, O2]): Stream[F2, (O, O2)] =
    zipWith(that)(Tuple2.apply)

  /** Like `zip`, but selects the right values only.
    * Useful with timed streams, the example below will emit a number every 100 milliseconds.
    *
    * @example {{{
    * scala> import scala.concurrent.duration._, cats.effect.IO, cats.effect.unsafe.implicits.global
    * scala> val s = Stream.fixedDelay[IO](100.millis) zipRight Stream.range(0, 5)
    * scala> s.compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(0, 1, 2, 3, 4)
    * }}}
    */
  def zipRight[F2[x] >: F[x], O2](that: Stream[F2, O2]): Stream[F2, O2] =
    zipWith(that)((_, y) => y)

  /** Like `zip`, but selects the left values only.
    * Useful with timed streams, the example below will emit a number every 100 milliseconds.
    *
    * @example {{{
    * scala> import scala.concurrent.duration._, cats.effect.IO, cats.effect.unsafe.implicits.global
    * scala> val s = Stream.range(0, 5) zipLeft Stream.fixedDelay[IO](100.millis)
    * scala> s.compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(0, 1, 2, 3, 4)
    * }}}
    */
  def zipLeft[F2[x] >: F[x], O2](that: Stream[F2, O2]): Stream[F2, O] =
    zipWith(that)((x, _) => x)

  /** Determinsitically zips elements using the specified function,
    * terminating when the end of either branch is reached naturally.
    *
    * @example {{{
    * scala> Stream(1, 2, 3).zipWith(Stream(4, 5, 6, 7))(_ + _).toList
    * res0: List[Int] = List(5, 7, 9)
    * }}}
    */
  def zipWith[F2[x] >: F[x], O2 >: O, O3, O4](
      that: Stream[F2, O3]
  )(f: (O2, O3) => O4): Stream[F2, O4] =
    zipWith_[F2, O2, O3, O4](that)((_, _) => Pull.done, (_, _) => Pull.done, _ => Pull.done)(f)

  /** Zips the elements of the input stream with its indices, and returns the new stream.
    *
    * @example {{{
    * scala> Stream("The", "quick", "brown", "fox").zipWithIndex.toList
    * res0: List[(String,Long)] = List((The,0), (quick,1), (brown,2), (fox,3))
    * }}}
    */
  def zipWithIndex: Stream[F, (O, Long)] =
    this.scanChunks(0L) { (index, c) =>
      var idx = index
      val out = c.map { o =>
        val r = (o, idx)
        idx += 1
        r
      }
      (idx, out)
    }

  /** Zips each element of this stream with the next element wrapped into `Some`.
    * The last element is zipped with `None`.
    *
    * @example {{{
    * scala> Stream("The", "quick", "brown", "fox").zipWithNext.toList
    * res0: List[(String,Option[String])] = List((The,Some(quick)), (quick,Some(brown)), (brown,Some(fox)), (fox,None))
    * }}}
    */
  def zipWithNext: Stream[F, (O, Option[O])] = {
    def go(last: O, s: Stream[F, O]): Pull[F, (O, Option[O]), Unit] =
      s.pull.uncons.flatMap {
        case None => Pull.output1((last, None))
        case Some((hd, tl)) =>
          val (newLast, out) = hd.mapAccumulate(last) { case (prev, next) =>
            (next, (prev, Some(next)))
          }
          Pull.output(out) >> go(newLast, tl)
      }
    this.pull.uncons1.flatMap {
      case Some((hd, tl)) => go(hd, tl)
      case None           => Pull.done
    }.stream
  }

  /** Zips each element of this stream with the previous element wrapped into `Some`.
    * The first element is zipped with `None`.
    *
    * @example {{{
    * scala> Stream("The", "quick", "brown", "fox").zipWithPrevious.toList
    * res0: List[(Option[String],String)] = List((None,The), (Some(The),quick), (Some(quick),brown), (Some(brown),fox))
    * }}}
    */
  def zipWithPrevious: Stream[F, (Option[O], O)] =
    mapAccumulate[Option[O], (Option[O], O)](None) { case (prev, next) =>
      (Some(next), (prev, next))
    }.map { case (_, prevNext) => prevNext }

  /** Zips each element of this stream with its previous and next element wrapped into `Some`.
    * The first element is zipped with `None` as the previous element,
    * the last element is zipped with `None` as the next element.
    *
    * @example {{{
    * scala> Stream("The", "quick", "brown", "fox").zipWithPreviousAndNext.toList
    * res0: List[(Option[String],String,Option[String])] = List((None,The,Some(quick)), (Some(The),quick,Some(brown)), (Some(quick),brown,Some(fox)), (Some(brown),fox,None))
    * }}}
    */
  def zipWithPreviousAndNext: Stream[F, (Option[O], O, Option[O])] =
    zipWithPrevious.zipWithNext.map {
      case ((prev, that), None)            => (prev, that, None)
      case ((prev, that), Some((_, next))) => (prev, that, Some(next))
    }

  /** Zips the input with a running total according to `S`, up to but not including the current element. Thus the initial
    * `z` value is the first emitted to the output:
    *
    * @example {{{
    * scala> Stream("uno", "dos", "tres", "cuatro").zipWithScan(0)(_ + _.length).toList
    * res0: List[(String,Int)] = List((uno,0), (dos,3), (tres,6), (cuatro,10))
    * }}}
    * @see [[zipWithScan1]]
    */
  def zipWithScan[O2](z: O2)(f: (O2, O) => O2): Stream[F, (O, O2)] =
    this
      .mapAccumulate(z) { (s, o) =>
        val s2 = f(s, o)
        (s2, (o, s))
      }
      .map(_._2)

  /** Zips the input with a running total according to `S`, including the current element. Thus the initial
    * `z` value is the first emitted to the output:
    *
    * @example {{{
    * scala> Stream("uno", "dos", "tres", "cuatro").zipWithScan1(0)(_ + _.length).toList
    * res0: List[(String, Int)] = List((uno,3), (dos,6), (tres,10), (cuatro,16))
    * }}}
    * @see [[zipWithScan]]
    */
  def zipWithScan1[O2](z: O2)(f: (O2, O) => O2): Stream[F, (O, O2)] =
    this
      .mapAccumulate(z) { (s, o) =>
        val s2 = f(s, o)
        (s2, (o, s2))
      }
      .map(_._2)

  override def toString: String = "Stream(..)"
}

object Stream extends StreamLowPriority {

  /** Creates a pure stream that emits the supplied values. To convert to an effectful stream, use `covary`. */
  def apply[F[x] >: Pure[x], O](os: O*): Stream[F, O] = emits(os)

  /** Creates a single element stream that gets its value by evaluating the supplied effect. If the effect fails, a `Left`
    * is emitted. Otherwise, a `Right` is emitted.
    *
    * Use [[eval]] instead if a failure while evaluating the effect should fail the stream.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream.attemptEval(SyncIO(10)).compile.toVector.unsafeRunSync()
    * res0: Vector[Either[Throwable,Int]] = Vector(Right(10))
    * scala> Stream.attemptEval(SyncIO(throw new RuntimeException)).compile.toVector.unsafeRunSync()
    * res1: Vector[Either[Throwable,Nothing]] = Vector(Left(java.lang.RuntimeException))
    * }}}
    */
  def attemptEval[F[x] >: Pure[x], O](fo: F[O]): Stream[F, Either[Throwable, O]] =
    new Stream(Pull.attemptEval(fo).flatMap(Pull.output1))

  /** Light weight alternative to `awakeEvery` that sleeps for duration `d` before each pulled element.
    */
  def awakeDelay[F[_]](
      period: FiniteDuration
  )(implicit t: Temporal[F]): Stream[F, FiniteDuration] =
    Stream.eval(t.monotonic).flatMap { start =>
      fixedDelay[F](period) >> Stream.eval(t.monotonic.map(_ - start))
    }

  /** Discrete stream that every `d` emits elapsed duration
    * since the start time of stream consumption.
    *
    * Missed periods are dampened to a single tick.
    *
    * For example: `awakeEvery[IO](5 seconds)` will
    * return (approximately) `5s, 10s, 15s`, and will lie dormant
    * between emitted values.
    *
    * @param period duration between emits of the resulting stream
    */
  def awakeEvery[F[_]: Temporal](period: FiniteDuration): Stream[F, FiniteDuration] =
    awakeEvery(period, true)

  /** Discrete stream that every `d` emits elapsed duration
    * since the start time of stream consumption.
    *
    * For example: `awakeEvery[IO](5 seconds)` will
    * return (approximately) `5s, 10s, 15s`, and will lie dormant
    * between emitted values.
    *
    * @param period duration between emits of the resulting stream
    * @param dampen whether missed periods result in 1 emitted tick or 1 per missed period, see `fixedRate` for more info
    */
  def awakeEvery[F[_]](
      period: FiniteDuration,
      dampen: Boolean
  )(implicit t: Temporal[F]): Stream[F, FiniteDuration] =
    Stream.eval(t.monotonic).flatMap { start =>
      fixedRate_[F](period, start, dampen) >> Stream.eval(
        t.monotonic.map(_ - start)
      )
    }

  /** Creates a stream that emits a resource allocated by an effect, ensuring the resource is
    * eventually released regardless of how the stream is used.
    *
    * A typical use case for bracket is working with files or network sockets. The resource effect
    * opens a file and returns a reference to it. One can then flatMap on the returned Stream to access
    *  the file, e.g to read bytes and transform them in to some stream of elements
    * (e.g., bytes, strings, lines, etc.).
    * The `release` action then closes the file once the result Stream terminates, even in case of interruption
    * or errors.
    *
    * @param acquire resource to acquire at start of stream
    * @param release function which returns an effect that releases the resource
    */
  def bracket[F[x] >: Pure[x], R](acquire: F[R])(release: R => F[Unit]): Stream[F, R] =
    bracketCase(acquire)((r, _) => release(r))

  /** Like [[bracket]] but no scope is introduced, causing resource finalization to
    * occur at the end of the current scope at the time of acquisition.
    */
  def bracketWeak[F[x] >: Pure[x], R](acquire: F[R])(release: R => F[Unit]): Stream[F, R] =
    bracketCaseWeak(acquire)((r, _) => release(r))

  /** Like [[bracket]] but the release action is passed an `ExitCase[Throwable]`.
    *
    * `ExitCase.Canceled` is passed to the release action in the event of either stream interruption or
    * overall compiled effect cancelation.
    */
  def bracketCase[F[x] >: Pure[x], R](
      acquire: F[R]
  )(release: (R, Resource.ExitCase) => F[Unit]): Stream[F, R] =
    bracketCaseWeak(acquire)(release).scope

  /** Like [[bracketCase]] but no scope is introduced, causing resource finalization to
    * occur at the end of the current scope at the time of acquisition.
    */
  def bracketCaseWeak[F[x] >: Pure[x], R](
      acquire: F[R]
  )(release: (R, Resource.ExitCase) => F[Unit]): Stream[F, R] =
    new Stream(Pull.acquire[F, R](acquire, release).flatMap(Pull.output1(_)))

  /** Like [[bracketCase]] but the acquire action may be canceled.
    */
  def bracketFull[F[x] >: Pure[x], R](
      acquire: Poll[F] => F[R]
  )(release: (R, Resource.ExitCase) => F[Unit])(implicit
      F: MonadCancel[F, _]
  ): Stream[F, R] =
    bracketFullWeak(acquire)(release).scope

  /** Like [[bracketFull]] but no scope is introduced, causing resource finalization to
    * occur at the end of the current scope at the time of acquisition.
    */
  def bracketFullWeak[F[x] >: Pure[x], R](
      acquire: Poll[F] => F[R]
  )(release: (R, Resource.ExitCase) => F[Unit])(implicit
      F: MonadCancel[F, _]
  ): Stream[F, R] =
    new Stream(Pull.acquireCancelable[F, R](acquire, release).flatMap(Pull.output1))

  /** Creates a pure stream that emits the elements of the supplied chunk.
    *
    * @example {{{
    * scala> Stream.chunk(Chunk(1,2,3)).toList
    * res0: List[Int] = List(1, 2, 3)
    * }}}
    */
  def chunk[F[x] >: Pure[x], O](os: Chunk[O]): Stream[F, O] =
    new Stream(Pull.output(os))

  /** Creates an infinite pure stream that always returns the supplied value.
    *
    * Elements are emitted in finite chunks with `chunkSize` number of elements.
    *
    * @example {{{
    * scala> Stream.constant(0).take(5).toList
    * res0: List[Int] = List(0, 0, 0, 0, 0)
    * }}}
    */
  def constant[F[x] >: Pure[x], O](o: O, chunkSize: Int = 256): Stream[F, O] =
    chunk(Chunk.seq(List.fill(chunkSize)(o))).repeat

  /** A continuous stream of the elapsed time, computed using `System.nanoTime`.
    * Note that the actual granularity of these elapsed times depends on the OS, for instance
    * the OS may only update the current time every ten milliseconds or so.
    */
  def duration[F[_]](implicit F: Clock[F]): Stream[F, FiniteDuration] = {
    implicit val applicativeF: Applicative[F] = F.applicative
    Stream.eval(F.monotonic).flatMap { t0 =>
      Stream.repeatEval(F.monotonic.map(t => t - t0))
    }
  }

  /** Creates a singleton pure stream that emits the supplied value.
    *
    * @example {{{
    * scala> Stream.emit(0).toList
    * res0: List[Int] = List(0)
    * }}}
    */
  def emit[F[x] >: Pure[x], O](o: O): Stream[F, O] = new Stream(Pull.output1(o))

  /** Creates a pure stream that emits the supplied values.
    *
    * @example {{{
    * scala> Stream.emits(List(1, 2, 3)).toList
    * res0: List[Int] = List(1, 2, 3)
    * }}}
    */
  def emits[F[x] >: Pure[x], O](os: scala.collection.Seq[O]): Stream[F, O] =
    os match {
      case Nil               => empty
      case collection.Seq(x) => emit(x)
      case _                 => new Stream(Pull.output(Chunk.seq(os)))
    }

  /** Empty pure stream. */
  val empty: Stream[Pure, INothing] =
    new Stream(Pull.done)

  /** Creates a single element stream that gets its value by evaluating the supplied effect. If the effect fails,
    * the returned stream fails.
    *
    * Use [[attemptEval]] instead if a failure while evaluating the effect should be emitted as a value.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream.eval(SyncIO(10)).compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(10)
    * scala> Stream.eval(SyncIO(throw new RuntimeException)).covaryOutput[Int].compile.toVector.attempt.unsafeRunSync()
    * res1: Either[Throwable,Vector[Int]] = Left(java.lang.RuntimeException)
    * }}}
    */
  def eval[F[_], O](fo: F[O]): Stream[F, O] =
    new Stream(Pull.eval(fo).flatMap(Pull.output1))

  /** Creates a stream that evaluates the supplied `fa` for its effect, discarding the output value.
    * As a result, the returned stream emits no elements and hence has output type `INothing`.
    *
    * Alias for `eval(fa).drain`.
    */
  @deprecated("Use exec if passing an F[Unit] or eval(fa).drain if passing an F[A]", "2.5.0")
  def eval_[F[_], A](fa: F[A]): Stream[F, INothing] =
    new Stream(Pull.eval(fa).map(_ => ()))

  /** Like `eval` but resulting chunk is flatten efficiently. */
  def evalUnChunk[F[_], O](fo: F[Chunk[O]]): Stream[F, O] =
    new Stream(Pull.eval(fo).flatMap(Pull.output(_)))

  /** Like `eval`, but lifts a foldable structure. */
  def evals[F[_], S[_]: Foldable, O](fo: F[S[O]]): Stream[F, O] =
    eval(fo).flatMap(so => Stream.emits(so.toList))

  /** Like `evals`, but lifts any Seq in the effect. */
  def evalSeq[F[_], S[A] <: Seq[A], O](fo: F[S[O]]): Stream[F, O] =
    eval(fo).flatMap(Stream.emits)

  /** A continuous stream which is true after `d, 2d, 3d...` elapsed duration,
    * and false otherwise.
    * If you'd like a 'discrete' stream that will actually block until `d` has elapsed,
    * use `awakeEvery` instead.
    */
  def every[F[_]](
      d: FiniteDuration
  )(implicit clock: Clock[F], F: Functor[F]): Stream[F, Boolean] = {
    def go(lastSpikeNanos: Long): Stream[F, Boolean] =
      Stream.eval(clock.monotonic.map(_.toNanos)).flatMap { now =>
        if ((now - lastSpikeNanos) > d.toNanos) Stream.emit(true) ++ go(now)
        else Stream.emit(false) ++ go(lastSpikeNanos)
      }
    go(0)
  }

  /** As a result, the returned stream emits no elements and hence has output type `INothing`.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream.exec(SyncIO(println("Ran"))).covaryOutput[Int].compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector()
    * }}}
    */
  def exec[F[_]](action: F[Unit]): Stream[F, INothing] =
    new Stream(Pull.eval(action))

  /** Light weight alternative to [[fixedRate]] that sleeps for duration `d` before each pulled element.
    *
    * Behavior differs from `fixedRate` because the sleep between elements occurs after the next element
    * is pulled whereas `fixedRate` accounts for the time it takes to process the emitted unit.
    * This difference can roughly be thought of as the difference between `scheduleWithFixedDelay` and
    * `scheduleAtFixedRate` in `java.util.concurrent.Scheduler`.
    *
    * Alias for `sleep(period).repeat`.
    */
  def fixedDelay[F[_]](period: FiniteDuration)(implicit t: Temporal[F]): Stream[F, Unit] =
    sleep(period).repeat

  /** Discrete stream that emits a unit every `d`, with missed period ticks dampened.
    *
    * See [[fixedDelay]] for an alternative that sleeps `d` between elements.
    *
    * @param period duration between emits of the resulting stream
    */
  def fixedRate[F[_]](period: FiniteDuration)(implicit t: Temporal[F]): Stream[F, Unit] =
    fixedRate(period, true)

  /** Discrete stream that emits a unit every `d`.
    *
    * See [[fixedDelay]] for an alternative that sleeps `d` between elements.
    *
    * This operation differs in that the time between ticks should roughly be equal to the specified period, regardless
    * of how much time it takes to process that tick downstream. For example, with a 1 second period and a task that takes 100ms,
    * the task would run at timestamps, 1s, 2s, 3s, etc. when using `fixedRate >> task` whereas it would run at timestamps
    * 1s, 2.1s, 3.2s, etc. when using `fixedDelay >> task`.
    *
    * In the case where task processing takes longer than a single period, 1 or more ticks are immediately emitted to "catch-up".
    * The `dampen` parameter controls whether a single tick is emitted or whether one per missed period is emitted.
    *
    * @param period period between emits of the resulting stream
    * @param dampen true if a single unit should be emitted when multiple periods have passed since last execution, false if a unit for each period should be emitted
    */
  def fixedRate[F[_]](period: FiniteDuration, dampen: Boolean)(implicit
      F: Temporal[F]
  ): Stream[F, Unit] =
    Stream.eval(F.monotonic).flatMap(t => fixedRate_(period, t, dampen))

  private def fixedRate_[F[_]: Temporal](
      period: FiniteDuration,
      lastAwakeAt: FiniteDuration,
      dampen: Boolean
  ): Stream[F, Unit] =
    if (period.toNanos == 0) Stream(()).repeat
    else
      Stream.eval(Temporal[F].monotonic).flatMap { now =>
        val next = lastAwakeAt + period
        if (next > now)
          Stream.sleep(next - now) ++ fixedRate_(period, next, dampen)
        else {
          val ticks = (now.toNanos - lastAwakeAt.toNanos - 1) / period.toNanos
          val step =
            ticks match {
              case count if count < 0            => Stream.empty
              case count if count == 0 || dampen => Stream.emit(())
              case count                         => Stream.emit(()).repeatN(count)
            }
          step ++ fixedRate_(period, lastAwakeAt + (period * ticks), dampen)
        }
      }

  private[fs2] final class PartiallyAppliedFromOption[F[_]](
      private val dummy: Boolean
  ) extends AnyVal {
    def apply[A](option: Option[A]): Stream[F, A] =
      option.map(Stream.emit).getOrElse(Stream.empty)
  }

  /** Lifts an Option[A] to an effectful Stream.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream.fromOption[SyncIO](Some(42)).compile.toList.unsafeRunSync()
    * res0: List[Int] = List(42)
    * scala> Stream.fromOption[SyncIO](None).compile.toList.unsafeRunSync()
    * res1: List[Nothing] = List()
    * }}}
    */
  def fromOption[F[_]]: PartiallyAppliedFromOption[F] =
    new PartiallyAppliedFromOption(dummy = true)

  private[fs2] final class PartiallyAppliedFromEither[F[_]](
      private val dummy: Boolean
  ) extends AnyVal {
    def apply[A](either: Either[Throwable, A])(implicit ev: RaiseThrowable[F]): Stream[F, A] =
      either.fold(Stream.raiseError[F], Stream.emit)
  }

  /** Lifts an Either[Throwable, A] to an effectful Stream.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO, scala.util.Try
    * scala> Stream.fromEither[SyncIO](Right(42)).compile.toList.unsafeRunSync()
    * res0: List[Int] = List(42)
    * scala> Try(Stream.fromEither[SyncIO](Left(new RuntimeException)).compile.toList.unsafeRunSync())
    * res1: Try[List[Nothing]] = Failure(java.lang.RuntimeException)
    * }}}
    */
  def fromEither[F[_]]: PartiallyAppliedFromEither[F] =
    new PartiallyAppliedFromEither(dummy = true)

  private[fs2] final class PartiallyAppliedFromIterator[F[_]](
      private val blocking: Boolean
  ) extends AnyVal {
    def apply[A](iterator: Iterator[A], chunkSize: Int)(implicit F: Sync[F]): Stream[F, A] = {
      def eff[B](thunk: => B) = if (blocking) F.blocking(thunk) else F.delay(thunk)

      def getNextChunk(i: Iterator[A]): F[Option[(Chunk[A], Iterator[A])]] =
        eff {
          for (_ <- 1 to chunkSize if i.hasNext) yield i.next()
        }.map { s =>
          if (s.isEmpty) None else Some((Chunk.seq(s), i))
        }

      Stream.unfoldChunkEval(iterator)(getNextChunk)
    }
  }

  /** Lifts an iterator into a Stream.
    */
  def fromIterator[F[_]]: PartiallyAppliedFromIterator[F] =
    new PartiallyAppliedFromIterator(blocking = false)

  /** Lifts an iterator into a Stream, shifting any interaction with the iterator to the blocking pool.
    */
  def fromBlockingIterator[F[_]]: PartiallyAppliedFromIterator[F] =
    new PartiallyAppliedFromIterator(blocking = true)

  /** Returns a stream of elements from the supplied queue.
    *
    * All elements that are available, up to the specified limit,
    * are dequeued and emitted as a single chunk.
    */
  def fromQueueUnterminated[F[_]: Functor, A](
      queue: QueueSource[F, A],
      limit: Int = Int.MaxValue
  ): Stream[F, A] =
    fromQueueNoneTerminatedChunk_[F, A](
      queue.take.map(a => Some(Chunk.singleton(a))),
      queue.tryTake.map(_.map(a => Some(Chunk.singleton(a)))),
      limit
    )

  /** Returns a stream of elements from the supplied queue.
    *
    * All elements that are available, up to the specified limit,
    * are dequeued and emitted as a single chunk.
    */
  def fromQueueUnterminated[F[_]: Functor, A](
      queue: Queue[F, A],
      limit: Int
  ): Stream[F, A] = fromQueueUnterminated(queue: QueueSource[F, A], limit)

  /** Returns a stream of elements from the supplied queue.
    *
    * All elements that are available, up to the specified limit,
    * are dequeued and emitted as a single chunk.
    */
  def fromQueueUnterminatedChunk[F[_]: Functor, A](
      queue: QueueSource[F, Chunk[A]],
      limit: Int = Int.MaxValue
  ): Stream[F, A] =
    fromQueueNoneTerminatedChunk_[F, A](
      queue.take.map(Some(_)),
      queue.tryTake.map(_.map(Some(_))),
      limit
    )

  /** Returns a stream of elements from the supplied queue.
    *
    * All elements that are available, up to the specified limit,
    * are dequeued and emitted as a single chunk.
    */
  def fromQueueUnterminatedChunk[F[_]: Functor, A](
      queue: Queue[F, Chunk[A]],
      limit: Int
  ): Stream[F, A] = fromQueueUnterminatedChunk(queue: QueueSource[F, Chunk[A]], limit)

  /** Returns a stream of elements from the supplied queue.
    *
    * The stream terminates upon dequeuing a `None`.
    *
    * All elements that are available, up to the specified limit,
    * are dequeued and emitted as a single chunk.
    */
  def fromQueueNoneTerminated[F[_]: Functor, A](
      queue: QueueSource[F, Option[A]],
      limit: Int = Int.MaxValue
  ): Stream[F, A] =
    fromQueueNoneTerminatedChunk_(
      queue.take.map(_.map(Chunk.singleton)),
      queue.tryTake.map(_.map(_.map(Chunk.singleton))),
      limit
    )

  /** Returns a stream of elements from the supplied queue.
    *
    * The stream terminates upon dequeuing a `None`.
    *
    * All elements that are available, up to the specified limit,
    * are dequeued and emitted as a single chunk.
    */
  def fromQueueNoneTerminated[F[_]: Functor, A](
      queue: Queue[F, Option[A]],
      limit: Int
  ): Stream[F, A] = fromQueueNoneTerminated(queue: QueueSource[F, Option[A]], limit)

  /** Returns a stream of elements from the supplied queue.
    *
    * The stream terminates upon dequeuing a `None`.
    *
    * All elements that are available, up to the specified limit,
    * are dequeued and emitted as a single chunk.
    */
  def fromQueueNoneTerminatedChunk[F[_], A](
      queue: QueueSource[F, Option[Chunk[A]]],
      limit: Int = Int.MaxValue
  ): Stream[F, A] =
    fromQueueNoneTerminatedChunk_(queue.take, queue.tryTake, limit)

  /** Returns a stream of elements from the supplied queue.
    *
    * The stream terminates upon dequeuing a `None`.
    *
    * All elements that are available, up to the specified limit,
    * are dequeued and emitted as a single chunk.
    */
  def fromQueueNoneTerminatedChunk[F[_], A](
      queue: Queue[F, Option[Chunk[A]]],
      limit: Int
  ): Stream[F, A] = fromQueueNoneTerminatedChunk(queue: QueueSource[F, Option[Chunk[A]]], limit)

  private def fromQueueNoneTerminatedChunk_[F[_], A](
      take: F[Option[Chunk[A]]],
      tryTake: F[Option[Option[Chunk[A]]]],
      limit: Int
  ): Stream[F, A] = {
    def await: Stream[F, A] =
      Stream.eval(take).flatMap {
        case None    => Stream.empty
        case Some(c) => pump(c)
      }
    def pump(acc: Chunk[A]): Stream[F, A] = {
      val sz = acc.size
      if (sz > limit) {
        val (pfx, sfx) = acc.splitAt(limit)
        Stream.chunk(pfx) ++ pump(sfx)
      } else if (sz == limit) Stream.chunk(acc) ++ await
      else
        Stream.eval(tryTake).flatMap {
          case None          => Stream.chunk(acc) ++ await
          case Some(Some(c)) => pump(acc ++ c)
          case Some(None)    => Stream.chunk(acc)
        }
    }
    await
  }

  /** Like `emits`, but works for any G that has a `Foldable` instance.
    */
  def foldable[F[x] >: Pure[x], G[_]: Foldable, O](os: G[O]): Stream[F, O] =
    Stream.emits(os.toList)

  /** Lifts an effect that generates a stream in to a stream. Alias for `eval(f).flatMap(_)`.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream.force(SyncIO(Stream(1,2,3).covary[SyncIO])).compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(1, 2, 3)
    * }}}
    */
  def force[F[_], A](f: F[Stream[F, A]]): Stream[F, A] =
    eval(f).flatMap(s => s)

  /** Like `emits`, but works for any class that extends `Iterable`
    */
  def iterable[F[x] >: Pure[x], A](os: Iterable[A]): Stream[F, A] =
    Stream.chunk(Chunk.iterable(os))

  /** An infinite `Stream` that repeatedly applies a given function
    * to a start value. `start` is the first value emitted, followed
    * by `f(start)`, then `f(f(start))`, and so on.
    *
    * @example {{{
    * scala> Stream.iterate(0)(_ + 1).take(10).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    * }}}
    */
  def iterate[F[x] >: Pure[x], A](start: A)(f: A => A): Stream[F, A] =
    emit(start) ++ iterate(f(start))(f)

  /** Like [[iterate]], but takes an effectful function for producing
    * the next state. `start` is the first value emitted.
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream.iterateEval(0)(i => SyncIO(i + 1)).take(10).compile.toVector.unsafeRunSync()
    * res0: Vector[Int] = Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    * }}}
    */
  def iterateEval[F[_], A](start: A)(f: A => F[A]): Stream[F, A] =
    emit(start) ++ eval(f(start)).flatMap(iterateEval(_)(f))

  /** A stream that never emits and never terminates.
    */
  def never[F[_]](implicit F: Spawn[F]): Stream[F, Nothing] =
    Stream.eval(F.never)

  /** Creates a stream that, when run, fails with the supplied exception.
    *
    * The `F` type must be explicitly provided (e.g., via `raiseError[IO]` or `raiseError[Fallible]`).
    *
    * @example {{{
    * scala> import cats.effect.SyncIO
    * scala> Stream.raiseError[Fallible](new RuntimeException).toList
    * res0: Either[Throwable,List[INothing]] = Left(java.lang.RuntimeException)
    * scala> Stream.raiseError[SyncIO](new RuntimeException).covaryOutput[Int].compile.drain.attempt.unsafeRunSync()
    * res0: Either[Throwable,Unit] = Left(java.lang.RuntimeException)
    * }}}
    */
  def raiseError[F[_]: RaiseThrowable](e: Throwable): Stream[F, INothing] =
    new Stream(Pull.raiseError(e))

  /** Lazily produces the sequence `[start, start + 1, start + 2, ..., stopExclusive)`.
    * If you want to produce the sequence in one chunk, instead of lazily, use
    * `emits(start until stopExclusive)`.
    *
    * @example {{{
    * scala> Stream.range(10, 20).toList
    * res0: List[Int] = List(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    * }}}
    */
  def range[F[x] >: Pure[x], O: Numeric](start: O, stopExclusive: O): Stream[F, O] =
    range(start, stopExclusive, implicitly[Numeric[O]].one)

  /** Lazily produce the sequence `[start, start + step, start + 2 * step, ..., stopExclusive)`.
    * If you want to produce the sequence in one chunk, instead of lazily, use
    * `emits(start until stopExclusive by step)`.
    *
    * @example {{{
    * scala> Stream.range(10, 20, 2).toList
    * res0: List[Int] = List(10, 12, 14, 16, 18)
    * }}}
    */
  def range[F[x] >: Pure[x], O: Numeric](start: O, stopExclusive: O, step: O): Stream[F, O] = {
    import Numeric.Implicits._
    import Ordering.Implicits._
    val zero = implicitly[Numeric[O]].zero
    def go(o: O): Stream[F, O] =
      if (
        (step > zero && o < stopExclusive && start < stopExclusive) ||
        (step < zero && o > stopExclusive && start > stopExclusive)
      )
        emit(o) ++ go(o + step)
      else empty
    go(start)
  }

  /** Lazily produce a sequence of nonoverlapping ranges, where each range
    * contains `size` integers, assuming the upper bound is exclusive.
    * Example: `ranges(0, 1000, 10)` results in the pairs
    * `(0, 10), (10, 20), (20, 30) ... (990, 1000)`
    *
    * Note: The last emitted range may be truncated at `stopExclusive`. For
    * instance, `ranges(0,5,4)` results in `(0,4), (4,5)`.
    *
    * @example {{{
    * scala> Stream.ranges(0, 20, 5).toList
    * res0: List[(Int,Int)] = List((0,5), (5,10), (10,15), (15,20))
    * }}}
    * @throws IllegalArgumentException if `size` <= 0
    */
  def ranges[F[x] >: Pure[x]](start: Int, stopExclusive: Int, size: Int): Stream[F, (Int, Int)] = {
    require(size > 0, "size must be > 0, was: " + size)
    unfold(start) { lower =>
      if (lower < stopExclusive)
        Some((lower -> ((lower + size).min(stopExclusive)), lower + size))
      else
        None
    }
  }

  /** Alias for `eval(fo).repeat`. */
  def repeatEval[F[_], O](fo: F[O]): Stream[F, O] = eval(fo).repeat

  /** Converts the supplied resource in to a singleton stream. */
  def resource[F[_], O](r: Resource[F, O])(implicit F: MonadCancel[F, _]): Stream[F, O] =
    resourceWeak(r).scope

  /** Like [[resource]] but does not introduce a scope, allowing finalization to occur after
    * subsequent appends or other scope-preserving transformations.
    *
    * Scopes can be manually introduced via [[Stream#scope]] if desired.
    */
  def resourceWeak[F[_], O](r: Resource[F, O])(implicit F: MonadCancel[F, _]): Stream[F, O] =
    r match {
      case Resource.Allocate(resource) =>
        Stream
          .bracketFullWeak(resource) { case ((_, release), exit) =>
            release(exit)
          }
          .map(_._1)
      case Resource.Bind(source, f) =>
        resourceWeak(source).flatMap(o => resourceWeak(f(o)))
      case Resource.Eval(fo) => Stream.eval(fo)
      case Resource.Pure(o)  => Stream.emit(o)
    }

  /** Retries `fo` on failure, returning a singleton stream with the
    * result of `fo` as soon as it succeeds.
    *
    * @param delay Duration of delay before the first retry
    * @param nextDelay Applied to the previous delay to compute the
    *                  next, e.g. to implement exponential backoff
    * @param maxAttempts Number of attempts before failing with the
    *                   latest error, if `fo` never succeeds
    * @param retriable Function to determine whether a failure is
    *                  retriable or not, defaults to retry every
    *                  `NonFatal`. A failed stream is immediately
    *                  returned when a non-retriable failure is
    *                  encountered
    */
  def retry[F[_]: Temporal: RaiseThrowable, O](
      fo: F[O],
      delay: FiniteDuration,
      nextDelay: FiniteDuration => FiniteDuration,
      maxAttempts: Int,
      retriable: Throwable => Boolean = scala.util.control.NonFatal.apply
  ): Stream[F, O] = {
    assert(maxAttempts > 0, s"maxAttempts should > 0, was $maxAttempts")

    val delays = Stream.unfold(delay)(d => Some(d -> nextDelay(d))).covary[F]

    Stream
      .eval(fo)
      .attempts(delays)
      .take(maxAttempts.toLong)
      .takeThrough(_.fold(err => retriable(err), _ => false))
      .last
      .map(_.get)
      .rethrow
  }

  /** A single-element `Stream` that waits for the duration `d` before emitting unit.
    */
  def sleep[F[_]](d: FiniteDuration)(implicit t: Temporal[F]): Stream[F, Unit] =
    Stream.eval(t.sleep(d))

  /** Alias for `sleep(d).drain`. Often used in conjunction with `++` (i.e., `sleep_(..) ++ s`) as a more
    * performant version of `sleep(..) >> s`.
    */
  def sleep_[F[_]](d: FiniteDuration)(implicit t: Temporal[F]): Stream[F, INothing] =
    Stream.exec(t.sleep(d))

  /** Starts the supplied task and cancels it as finalization of the returned stream.
    */
  def supervise[F[_], A](
      fa: F[A]
  )(implicit F: Spawn[F]): Stream[F, Fiber[F, Throwable, A]] =
    bracket(F.start(fa))(_.cancel)

  /** Returns a stream that evaluates the supplied by-name each time the stream is used,
    * allowing use of a mutable value in stream computations.
    *
    * Note: it's generally easier to reason about such computations using effectful
    * values. That is, allocate the mutable value in an effect and then use
    * `Stream.eval(fa).flatMap { a => ??? }`.
    *
    * @example {{{
    * scala> Stream.suspend {
    *      |   val digest = java.security.MessageDigest.getInstance("SHA-256")
    *      |   val bytes: Stream[Pure,Byte] = ???
    *      |   bytes.chunks.fold(digest) { (d,c) => d.update(c.toBytes.values); d }
    *      | }
    * }}}
    */
  def suspend[F[_], O](s: => Stream[F, O]): Stream[F, O] =
    new Stream(Pull.suspend(s.underlying))

  /** Creates a stream by successively applying `f` until a `None` is returned, emitting
    * each output `O` and using each output `S` as input to the next invocation of `f`.
    *
    * @example {{{
    * scala> Stream.unfold(0)(i => if (i < 5) Some(i -> (i+1)) else None).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4)
    * }}}
    */
  def unfold[F[x] >: Pure[x], S, O](s: S)(f: S => Option[(O, S)]): Stream[F, O] = {
    def go(s: S): Stream[F, O] =
      f(s) match {
        case Some((o, s)) => emit(o) ++ go(s)
        case None         => empty
      }
    suspend(go(s))
  }

  /** Like [[unfold]] but each invocation of `f` provides a chunk of output.
    *
    * @example {{{
    * scala> Stream.unfoldChunk(0)(i => if (i < 5) Some(Chunk.seq(List.fill(i)(i)) -> (i+1)) else None).toList
    * res0: List[Int] = List(1, 2, 2, 3, 3, 3, 4, 4, 4, 4)
    * }}}
    */
  def unfoldChunk[F[x] >: Pure[x], S, O](s: S)(f: S => Option[(Chunk[O], S)]): Stream[F, O] =
    unfold(s)(f).flatMap(chunk)

  /** Like [[unfold]], but takes an effectful function. */
  def unfoldEval[F[_], S, O](s: S)(f: S => F[Option[(O, S)]]): Stream[F, O] = {
    def go(s: S): Stream[F, O] =
      eval(f(s)).flatMap {
        case Some((o, s)) => emit(o) ++ go(s)
        case None         => empty
      }
    suspend(go(s))
  }

  /** Like [[unfoldChunk]], but takes an effectful function. */
  def unfoldChunkEval[F[_], S, O](s: S)(f: S => F[Option[(Chunk[O], S)]]): Stream[F, O] = {
    def go(s: S): Stream[F, O] =
      eval(f(s)).flatMap {
        case Some((c, s)) => chunk(c) ++ go(s)
        case None         => empty
      }
    suspend(go(s))
  }

  /** Creates a stream by successively applying `f` to a `S`, emitting
    * each output `O` and using each output `S` as input to the next invocation of `f`
    * if it is Some, or terminating on None
    *
    * @example {{{
    * scala> Stream.unfoldLoop(0)(i => (i, if (i < 5) Some(i+1) else None)).toList
    * res0: List[Int] = List(0, 1, 2, 3, 4, 5)
    * }}}
    */
  def unfoldLoop[F[x] <: Pure[x], S, O](s: S)(f: S => (O, Option[S])): Stream[F, O] =
    Pull
      .loop[F, O, S] { s =>
        val (o, sOpt) = f(s)
        Pull.output1(o) >> Pull.pure(sOpt)
      }(s)
      .stream

  /** Like [[unfoldLoop]], but takes an effectful function. */
  def unfoldLoopEval[F[_], S, O](s: S)(f: S => F[(O, Option[S])]): Stream[F, O] =
    Pull
      .loop[F, O, S](s =>
        Pull.eval(f(s)).flatMap { case (o, sOpt) =>
          Pull.output1(o) >> Pull.pure(sOpt)
        }
      )(s)
      .stream

  /** Provides syntax for streams that are invariant in `F` and `O`. */
  implicit final class InvariantOps[F[_], O](private val self: Stream[F, O]) extends AnyVal {

    /** Lifts this stream to the specified effect type.
      *
      * @example {{{
      * scala> import cats.effect.IO
      * scala> Stream(1, 2, 3).covary[IO]
      * res0: Stream[IO,Int] = Stream(..)
      * }}}
      */
    def covary[F2[x] >: F[x]]: Stream[F2, O] = self

    /** Synchronously sends values through `p`.
      *
      * If `p` fails, then resulting stream will fail. If `p` halts the evaluation will halt too.
      *
      * Note that observe will only output full chunks of `O` that are known to be successfully processed
      * by `p`. So if `p` terminates/fails in the middle of chunk processing, the chunk will not be available
      * in resulting stream.
      *
      * Note that if your pipe can be represented by an `O => F[Unit]`, `evalTap` will provide much greater performance.
      *
      * @example {{{
      * scala> import cats.effect.IO, cats.effect.unsafe.implicits.global
      * scala> Stream(1, 2, 3).covary[IO].observe(_.printlns).map(_ + 1).compile.toVector.unsafeRunSync()
      * res0: Vector[Int] = Vector(2, 3, 4)
      * }}}
      */
    def observe(p: Pipe[F, O, INothing])(implicit F: Concurrent[F]): Stream[F, O] =
      observeAsync(1)(p)

    /** Send chunks through `p`, allowing up to `maxQueued` pending _chunks_ before blocking `s`. */
    def observeAsync(
        maxQueued: Int
    )(pipe: Pipe[F, O, INothing])(implicit F: Concurrent[F]): Stream[F, O] = Stream.force {
      for {
        guard <- Semaphore[F](maxQueued - 1L)
        outChan <- Channel.unbounded[F, Chunk[O]]
        sinkChan <- Channel.unbounded[F, Chunk[O]]
      } yield {

        val sinkIn: Stream[F, INothing] = {
          def go(s: Stream[F, O]): Pull[F, INothing, Unit] =
            s.pull.uncons.flatMap {
              case None           => Pull.eval(sinkChan.close.void)
              case Some((hd, tl)) => Pull.eval(sinkChan.send(hd) >> guard.acquire) >> go(tl)
            }

          go(self).stream
        }

        val sinkOut: Stream[F, O] = {
          def go(s: Stream[F, Chunk[O]]): Pull[F, O, Unit] =
            s.pull.uncons1.flatMap {
              case None =>
                Pull.eval(outChan.close.void)
              case Some((ch, rest)) =>
                Pull.output(ch) >> Pull.eval(outChan.send(ch)) >> go(rest)
            }

          go(sinkChan.stream).stream
        }

        val runner =
          sinkOut.through(pipe).concurrently(sinkIn) ++ Stream.exec(outChan.close.void)

        def outStream =
          outChan.stream
            .flatMap { chunk =>
              Stream.chunk(chunk) ++ Stream.exec(guard.release)
            }

        outStream.concurrently(runner)
      }
    }

    /** Observes this stream of `Either[L, R]` values with two pipes, one that
      * observes left values and another that observes right values.
      *
      * If either of `left` or `right` fails, then resulting stream will fail.
      * If either `halts` the evaluation will halt too.
      */
    def observeEither[L, R](
        left: Pipe[F, L, INothing],
        right: Pipe[F, R, INothing]
    )(implicit F: Concurrent[F], ev: O <:< Either[L, R]): Stream[F, Either[L, R]] = {
      val src = self.asInstanceOf[Stream[F, Either[L, R]]]
      src
        .observe(_.collect { case Left(l) => l }.through(left))
        .observe(_.collect { case Right(r) => r }.through(right))
    }

    /** Gets a projection of this stream that allows converting it to a `Pull` in a number of ways. */
    def pull: ToPull[F, O] = new ToPull[F, O](self)

    /** Repeatedly invokes `using`, running the resultant `Pull` each time, halting when a pull
      * returns `None` instead of `Some(nextStream)`.
      */
    def repeatPull[O2](
        f: Stream.ToPull[F, O] => Pull[F, O2, Option[Stream[F, O]]]
    ): Stream[F, O2] =
      Pull.loop(f.andThen(_.map(_.map(_.pull))))(pull).stream
  }

  implicit final class NothingStreamOps[F[_]](private val self: Stream[F, Nothing]) extends AnyVal {

    /** Converts a `Stream[F, Nothing]` to a `Stream[F, Unit]` which emits a single `()` after this stream completes.
      */
    def unitary: Stream[F, Unit] =
      self ++ Stream.emit(())
  }

  implicit final class OptionStreamOps[F[_], O](private val self: Stream[F, Option[O]])
      extends AnyVal {

    /** Filters any 'None'.
      *
      * @example {{{
      * scala> Stream(Some(1), Some(2), None, Some(3), None).unNone.toList
      * res0: List[Int] = List(1, 2, 3)
      * }}}
      */
    def unNone: Stream[F, O] =
      self.collect { case Some(o2) => o2 }

    /** Halts the input stream at the first `None`.
      *
      * @example {{{
      * scala> Stream(Some(1), Some(2), None, Some(3), None).unNoneTerminate.toList
      * res0: List[Int] = List(1, 2)
      * }}}
      */
    def unNoneTerminate: Stream[F, O] =
      self.repeatPull {
        _.uncons.flatMap {
          case None => Pull.pure(None)
          case Some((hd, tl)) =>
            hd.indexWhere(_.isEmpty) match {
              case Some(0)   => Pull.pure(None)
              case Some(idx) => Pull.output(hd.take(idx).map(_.get)).as(None)
              case None      => Pull.output(hd.map(_.get)).as(Some(tl))
            }
        }
      }
  }

  /** Provides syntax for streams of streams. */
  implicit final class NestedStreamOps[F[_], O](private val outer: Stream[F, Stream[F, O]])
      extends AnyVal {

    /** Nondeterministically merges a stream of streams (`outer`) in to a single stream,
      * opening at most `maxOpen` streams at any point in time.
      *
      * The outer stream is evaluated and each resulting inner stream is run concurrently,
      * up to `maxOpen` stream. Once this limit is reached, evaluation of the outer stream
      * is paused until one or more inner streams finish evaluating.
      *
      * When the outer stream stops gracefully, all inner streams continue to run,
      * resulting in a stream that will stop when all inner streams finish
      * their evaluation.
      *
      * When the outer stream fails, evaluation of all inner streams is interrupted
      * and the resulting stream will fail with same failure.
      *
      * When any of the inner streams fail, then the outer stream and all other inner
      * streams are interrupted, resulting in stream that fails with the error of the
      * stream that caused initial failure.
      *
      * Finalizers on each inner stream are run at the end of the inner stream,
      * concurrently with other stream computations.
      *
      * Finalizers on the outer stream are run after all inner streams have been pulled
      * from the outer stream but not before all inner streams terminate -- hence finalizers on the outer stream will run
      * AFTER the LAST finalizer on the very last inner stream.
      *
      * Finalizers on the returned stream are run after the outer stream has finished
      * and all open inner streams have finished.
      *
      * @param maxOpen    Maximum number of open inner streams at any time. Must be > 0.
      */
    def parJoin(
        maxOpen: Int
    )(implicit F: Concurrent[F]): Stream[F, O] = {
      assert(maxOpen > 0, "maxOpen must be > 0, was: " + maxOpen)
      val fstream: F[Stream[F, O]] = for {
        done <- SignallingRef(None: Option[Option[Throwable]])
        available <- Semaphore[F](maxOpen.toLong)
        // starts with 1 because outer stream is running by default
        running <- SignallingRef(1L)
        // sync queue assures we won't overload heap when resulting stream is not able to catchup with inner streams
        outputChan <- Channel.synchronous[F, Chunk[O]]
      } yield {
        // stops the join evaluation
        // all the streams will be terminated. If err is supplied, that will get attached to any error currently present
        def stop(rslt: Option[Throwable]): F[Unit] =
          done.update {
            case rslt0 @ Some(Some(err0)) =>
              rslt.fold[Option[Option[Throwable]]](rslt0) { err =>
                Some(Some(CompositeFailure(err0, err)))
              }
            case _ => Some(rslt)
          } >> outputChan.close.void

        def untilDone[A](str: Stream[F, A]) = str.interruptWhen(done.map(_.nonEmpty))

        val incrementRunning: F[Unit] = running.update(_ + 1)
        val decrementRunning: F[Unit] =
          running.modify { n =>
            val now = n - 1
            now -> (if (now == 0) stop(None) else F.unit)
          }.flatten

        // "block" and await until the `running` counter drops to zero.
        val awaitWhileRunning: F[Unit] = running.discrete.forall(_ > 0).compile.drain

        // signals that a running stream, either inner or ourer, finished with or without failure.
        def endWithResult(result: Either[Throwable, Unit]): F[Unit] =
          result match {
            case Right(()) => decrementRunning
            case Left(err) => stop(Some(err)) >> decrementRunning
          }

        def sendToChannel(str: Stream[F, O]) = str.chunks.foreach(x => outputChan.send(x).void)

        // runs one inner stream, each stream is forked.
        // terminates when killSignal is true
        // failures will be propagated through `done` Signal
        // note that supplied scope's resources must be leased before the inner stream forks the execution to another thread
        // and that it must be released once the inner stream terminates or fails.
        def runInner(inner: Stream[F, O], outerScope: Scope[F]): F[Unit] =
          F.uncancelable { _ =>
            outerScope.lease.flatMap { lease =>
              available.acquire >>
                incrementRunning >>
                F.start {
                  // Note that the `interrupt` must be AFTER the send to the sync channel,
                  // otherwise the process may hang to send last item while being interrupted
                  val backInsertions = untilDone(sendToChannel(inner))
                  for {
                    r <- backInsertions.compile.drain.attempt
                    cancelResult <- lease.cancel
                    _ <- available.release
                    _ <- endWithResult(CompositeFailure.fromResults(r, cancelResult))
                  } yield ()
                }.void
            }
          }

        def runInnerScope(inner: Stream[F, O]): Stream[F, INothing] =
          new Stream(Pull.getScope[F].flatMap((sc: Scope[F]) => Pull.eval(runInner(inner, sc))))

        // runs the outer stream, interrupts when kill == true, and then decrements the `running`
        def runOuter: F[Unit] =
          untilDone(outer.flatMap(runInnerScope)).compile.drain.attempt.flatMap(endWithResult)

        // awaits when all streams (outer + inner) finished,
        // and then collects result of the stream (outer + inner) execution
        def signalResult: F[Unit] = done.get.flatMap(_.flatten.fold[F[Unit]](F.unit)(F.raiseError))
        val endOuter: F[Unit] = stop(None) >> awaitWhileRunning >> signalResult

        val backEnqueue = Stream.bracket(F.start(runOuter))(_ => endOuter)

        backEnqueue >> outputChan.stream.flatMap(Stream.chunk)
      }

      Stream.eval(fstream).flatten
    }

    /** Like [[parJoin]] but races all inner streams simultaneously. */
    def parJoinUnbounded(implicit F: Concurrent[F]): Stream[F, O] =
      parJoin(Int.MaxValue)
  }

  /** Provides syntax for pure streams. */
  implicit final class PureOps[O](private val self: Stream[Pure, O]) extends AnyVal {

    /** Alias for covary, to be able to write `Stream.empty[X]`. */
    def apply[F[_]]: Stream[F, O] = covary

    /** Lifts this stream to the specified effect type. */
    def covary[F[_]]: Stream[F, O] = self

    /** Runs this pure stream and returns the emitted elements in a collection of the specified type. Note: this method is only available on pure streams. */
    def to(c: Collector[O]): c.Out =
      self.covary[SyncIO].compile.to(c).unsafeRunSync()

    /** Runs this pure stream and returns the emitted elements in a list. Note: this method is only available on pure streams. */
    def toList: List[O] = to(List)

    /** Runs this pure stream and returns the emitted elements in a vector. Note: this method is only available on pure streams. */
    def toVector: Vector[O] = to(Vector)
  }

  /** Provides syntax for pure pipes based on `cats.Id`. */
  implicit final class IdOps[O](private val self: Stream[Id, O]) extends AnyVal {
    private def idToApplicative[F[_]: Applicative]: Id ~> F =
      new (Id ~> F) { def apply[A](a: Id[A]) = a.pure[F] }

    def covaryId[F[_]: Applicative]: Stream[F, O] = self.translate(idToApplicative[F])
  }

  /** Provides syntax for fallible streams. */
  implicit final class FallibleOps[O](private val self: Stream[Fallible, O]) extends AnyVal {

    /** Lifts this stream to the specified effect type. */
    def lift[F[_]](implicit F: ApplicativeError[F, Throwable]): Stream[F, O] = {
      val _ = F
      self.asInstanceOf[Stream[F, O]]
    }

    /** Runs this fallible stream and returns the emitted elements in a collection of the specified type. Note: this method is only available on fallible streams. */
    def to(c: Collector[O]): Either[Throwable, c.Out] =
      lift[SyncIO].compile.to(c).attempt.unsafeRunSync()

    /** Runs this fallible stream and returns the emitted elements in a list. Note: this method is only available on fallible streams. */
    def toList: Either[Throwable, List[O]] = to(List)

    /** Runs this fallible stream and returns the emitted elements in a vector. Note: this method is only available on fallible streams. */
    def toVector: Either[Throwable, Vector[O]] = to(Vector)
  }

  /** Projection of a `Stream` providing various ways to get a `Pull` from the `Stream`. */
  final class ToPull[F[_], O] private[Stream] (
      private val self: Stream[F, O]
  ) extends AnyVal {

    /** Waits for a chunk of elements to be available in the source stream.
      * The ''non-empty''' chunk of elements along with a new stream are provided as the resource of the returned pull.
      * The new stream can be used for subsequent operations, like awaiting again.
      * A `None` is returned as the resource of the pull upon reaching the end of the stream.
      */
    def uncons: Pull[F, INothing, Option[(Chunk[O], Stream[F, O])]] =
      Pull.uncons(self.underlying).map {
        _.map { case (hd, tl) => (hd, new Stream(tl)) }
      }

    /** Like [[uncons]] but waits for a single element instead of an entire chunk. */
    def uncons1: Pull[F, INothing, Option[(O, Stream[F, O])]] =
      uncons.flatMap {
        case None => Pull.pure(None)
        case Some((hd, tl)) =>
          val ntl = if (hd.size == 1) tl else tl.cons(hd.drop(1))
          Pull.pure(Some(hd(0) -> ntl))
      }

    /** Like [[uncons]], but returns a chunk of no more than `n` elements.
      *
      * `Pull.pure(None)` is returned if the end of the source stream is reached.
      */
    def unconsLimit(n: Int): Pull[F, INothing, Option[(Chunk[O], Stream[F, O])]] = {
      require(n > 0)
      uncons.flatMap {
        case Some((hd, tl)) =>
          if (hd.size < n) Pull.pure(Some(hd -> tl))
          else {
            val (out, rem) = hd.splitAt(n)
            Pull.pure(Some(out -> tl.cons(rem)))
          }
        case None => Pull.pure(None)
      }
    }

    /** Like [[uncons]], but returns a chunk of exactly `n` elements, splitting chunk as necessary.
      *
      * `Pull.pure(None)` is returned if the end of the source stream is reached.
      */
    def unconsN(
        n: Int,
        allowFewer: Boolean = false
    ): Pull[F, INothing, Option[(Chunk[O], Stream[F, O])]] = {
      def go(
          acc: Chunk[O],
          n: Int,
          s: Stream[F, O]
      ): Pull[F, INothing, Option[(Chunk[O], Stream[F, O])]] =
        s.pull.uncons.flatMap {
          case None =>
            if (allowFewer && acc.nonEmpty)
              Pull.pure(Some((acc, Stream.empty)))
            else Pull.pure(None)
          case Some((hd, tl)) =>
            if (hd.size < n) go(acc ++ hd, n - hd.size, tl)
            else if (hd.size == n) Pull.pure(Some((acc ++ hd) -> tl))
            else {
              val (pfx, sfx) = hd.splitAt(n)
              Pull.pure(Some((acc ++ pfx) -> tl.cons(sfx)))
            }
        }
      if (n <= 0) Pull.pure(Some((Chunk.empty, self)))
      else go(Chunk.empty, n, self)
    }

    /** Drops the first `n` elements of this `Stream`, and returns the new `Stream`. */
    def drop(n: Long): Pull[F, INothing, Option[Stream[F, O]]] =
      if (n <= 0) Pull.pure(Some(self))
      else
        uncons.flatMap {
          case None => Pull.pure(None)
          case Some((hd, tl)) =>
            hd.size.toLong match {
              case m if m < n  => tl.pull.drop(n - m)
              case m if m == n => Pull.pure(Some(tl))
              case _           => Pull.pure(Some(tl.cons(hd.drop(n.toInt))))
            }
        }

    /** Like [[dropWhile]], but drops the first value which tests false. */
    def dropThrough(p: O => Boolean): Pull[F, INothing, Option[Stream[F, O]]] =
      dropWhile_(p, true)

    /** Drops elements of the this stream until the predicate `p` fails, and returns the new stream.
      * If defined, the first element of the returned stream will fail `p`.
      */
    def dropWhile(p: O => Boolean): Pull[F, INothing, Option[Stream[F, O]]] =
      dropWhile_(p, false)

    private def dropWhile_(
        p: O => Boolean,
        dropFailure: Boolean
    ): Pull[F, INothing, Option[Stream[F, O]]] =
      uncons.flatMap {
        case None => Pull.pure(None)
        case Some((hd, tl)) =>
          hd.indexWhere(o => !p(o)) match {
            case None => tl.pull.dropWhile_(p, dropFailure)
            case Some(idx) =>
              val toDrop = if (dropFailure) idx + 1 else idx
              Pull.pure(Some(tl.cons(hd.drop(toDrop))))
          }
      }

    /** Takes the first value output by this stream and returns it in the result of a pull.
      * If no value is output before the stream terminates, the pull is failed with a `NoSuchElementException`.
      * If more than 1 value is output, everything beyond the first is ignored.
      */
    def headOrError(implicit F: RaiseThrowable[F]): Pull[F, INothing, O] =
      uncons.flatMap {
        case None          => Pull.raiseError(new NoSuchElementException)
        case Some((hd, _)) => Pull.pure(hd(0))
      }

    /** Writes all inputs to the output of the returned `Pull`. */
    def echo: Pull[F, O, Unit] = self.underlying

    /** Reads a single element from the input and emits it to the output. */
    def echo1: Pull[F, O, Option[Stream[F, O]]] =
      uncons.flatMap {
        case None => Pull.pure(None)
        case Some((hd, tl)) =>
          val (pre, post) = hd.splitAt(1)
          Pull.output(pre).as(Some(tl.cons(post)))
      }

    /** Reads the next available chunk from the input and emits it to the output. */
    def echoChunk: Pull[F, O, Option[Stream[F, O]]] =
      uncons.flatMap {
        case None           => Pull.pure(None)
        case Some((hd, tl)) => Pull.output(hd).as(Some(tl))
      }

    /** Like `[[unconsN]]`, but leaves the buffered input unconsumed. */
    def fetchN(n: Int): Pull[F, INothing, Option[Stream[F, O]]] =
      unconsN(n).map(_.map { case (hd, tl) => tl.cons(hd) })

    /** Awaits the next available element where the predicate returns true. */
    def find(f: O => Boolean): Pull[F, INothing, Option[(O, Stream[F, O])]] =
      uncons.flatMap {
        case None => Pull.pure(None)
        case Some((hd, tl)) =>
          hd.indexWhere(f) match {
            case None => tl.pull.find(f)
            case Some(idx) if idx + 1 < hd.size =>
              val rem = hd.drop(idx + 1)
              Pull.pure(Some((hd(idx), tl.cons(rem))))
            case Some(idx) => Pull.pure(Some((hd(idx), tl)))
          }
      }

    /** Folds all inputs using an initial value `z` and supplied binary operator, and writes the final
      * result to the output of the supplied `Pull` when the stream has no more values.
      */
    def fold[O2](z: O2)(f: (O2, O) => O2): Pull[F, INothing, O2] =
      uncons.flatMap {
        case None => Pull.pure(z)
        case Some((hd, tl)) =>
          val acc = hd.foldLeft(z)(f)
          tl.pull.fold(acc)(f)
      }

    /** Folds all inputs using the supplied binary operator, and writes the final result to the output of
      * the supplied `Pull` when the stream has no more values.
      */
    def fold1[O2 >: O](f: (O2, O2) => O2): Pull[F, INothing, Option[O2]] =
      uncons.flatMap {
        case None => Pull.pure(None)
        case Some((hd, tl)) =>
          val fst: O2 = hd.drop(1).foldLeft(hd(0): O2)(f)
          tl.pull.fold(fst)(f).map(Some(_))
      }

    /** Writes a single `true` value if all input matches the predicate, `false` otherwise. */
    def forall(p: O => Boolean): Pull[F, INothing, Boolean] =
      uncons.flatMap {
        case None => Pull.pure(true)
        case Some((hd, tl)) =>
          if (hd.forall(p)) tl.pull.forall(p) else Pull.pure(false)
      }

    /** Returns the last element of the input, if non-empty. */
    def last: Pull[F, INothing, Option[O]] = {
      def go(prev: Option[O], s: Stream[F, O]): Pull[F, INothing, Option[O]] =
        s.pull.uncons.flatMap {
          case None           => Pull.pure(prev)
          case Some((hd, tl)) => go(hd.last, tl)
        }
      go(None, self)
    }

    /** Returns the last element of the input, if non-empty, otherwise fails the pull with a `NoSuchElementException`. */
    def lastOrError(implicit F: RaiseThrowable[F]): Pull[F, INothing, O] =
      last.flatMap {
        case None    => Pull.raiseError(new NoSuchElementException)
        case Some(o) => Pull.pure(o)
      }

    /** Like [[uncons]] but does not consume the chunk (i.e., the chunk is pushed back). */
    def peek: Pull[F, INothing, Option[(Chunk[O], Stream[F, O])]] =
      uncons.flatMap {
        case None           => Pull.pure(None)
        case Some((hd, tl)) => Pull.pure(Some((hd, tl.cons(hd))))
      }

    /** Like [[uncons1]] but does not consume the element (i.e., the element is pushed back). */
    def peek1: Pull[F, INothing, Option[(O, Stream[F, O])]] =
      uncons.flatMap {
        case None           => Pull.pure(None)
        case Some((hd, tl)) => Pull.pure(Some((hd(0), tl.cons(hd))))
      }

    /** Like `scan` but `f` is applied to each chunk of the source stream.
      * The resulting chunk is emitted while the resulting state is used in the
      * next invocation of `f`. The final state value is returned as the result of the pull.
      */
    def scanChunks[S, O2](init: S)(f: (S, Chunk[O]) => (S, Chunk[O2])): Pull[F, O2, S] =
      scanChunksOpt(init)(s => Some(c => f(s, c)))

    /** More general version of `scanChunks` where the current state (i.e., `S`) can be inspected
      * to determine if another chunk should be pulled or if the pull should terminate.
      * Termination is signaled by returning `None` from `f`. Otherwise, a function which consumes
      * the next chunk is returned wrapped in `Some`. The final state value is returned as the
      * result of the pull.
      */
    def scanChunksOpt[S, O2](
        init: S
    )(f: S => Option[Chunk[O] => (S, Chunk[O2])]): Pull[F, O2, S] = {
      def go(acc: S, s: Stream[F, O]): Pull[F, O2, S] =
        f(acc) match {
          case None => Pull.pure(acc)
          case Some(g) =>
            s.pull.uncons.flatMap {
              case Some((hd, tl)) =>
                val (s2, c) = g(hd)
                Pull.output(c) >> go(s2, tl)
              case None =>
                Pull.pure(acc)
            }
        }
      go(init, self)
    }

    /** Like `uncons`, but instead of performing normal `uncons`, this will
      * run the stream up to the first chunk available.
      * Useful when zipping multiple streams (legs) into one stream.
      * Assures that scopes are correctly held for each stream `leg`
      * independently of scopes from other legs.
      *
      * If you are not pulling from multiple streams, consider using `uncons`.
      */
    def stepLeg: Pull[F, INothing, Option[StepLeg[F, O]]] =
      Pull.getScope[F].flatMap { scope =>
        new StepLeg[F, O](Chunk.empty, scope.id, self.underlying).stepLeg
      }

    /** Emits the first `n` elements of the input. */
    def take(n: Long): Pull[F, O, Option[Stream[F, O]]] =
      if (n <= 0) Pull.pure(None)
      else
        uncons.flatMap {
          case None => Pull.pure(None)
          case Some((hd, tl)) =>
            hd.size.toLong match {
              case m if m < n  => Pull.output(hd) >> tl.pull.take(n - m)
              case m if m == n => Pull.output(hd).as(Some(tl))
              case _ =>
                val (pfx, sfx) = hd.splitAt(n.toInt)
                Pull.output(pfx).as(Some(tl.cons(sfx)))
            }
        }

    /** Emits the last `n` elements of the input. */
    def takeRight(n: Int): Pull[F, INothing, Chunk[O]] = {
      def go(acc: Chunk[O], s: Stream[F, O]): Pull[F, INothing, Chunk[O]] =
        s.pull.unconsN(n, true).flatMap {
          case None => Pull.pure(acc)
          case Some((hd, tl)) =>
            go(acc.drop(hd.size) ++ hd, tl)
        }
      if (n <= 0) Pull.pure(Chunk.empty)
      else go(Chunk.empty, self)
    }

    /** Like [[takeWhile]], but emits the first value which tests false. */
    def takeThrough(p: O => Boolean): Pull[F, O, Option[Stream[F, O]]] =
      takeWhile_(p, true)

    /** Emits the elements of the stream until the predicate `p` fails,
      * and returns the remaining `Stream`. If non-empty, the returned stream will have
      * a first element `i` for which `p(i)` is `false`.
      */
    def takeWhile(p: O => Boolean, takeFailure: Boolean = false): Pull[F, O, Option[Stream[F, O]]] =
      takeWhile_(p, takeFailure)

    private def takeWhile_(
        p: O => Boolean,
        takeFailure: Boolean
    ): Pull[F, O, Option[Stream[F, O]]] =
      uncons.flatMap {
        case None => Pull.pure(None)
        case Some((hd, tl)) =>
          hd.indexWhere(o => !p(o)) match {
            case None => Pull.output(hd) >> tl.pull.takeWhile_(p, takeFailure)
            case Some(idx) =>
              val toTake = if (takeFailure) idx + 1 else idx
              val (pfx, sfx) = hd.splitAt(toTake)
              Pull.output(pfx) >> Pull.pure(Some(tl.cons(sfx)))
          }
      }

    /** Allows expressing `Pull` computations whose `uncons` can receive
      * a user-controlled, resettable `timeout`.
      * See [[Pull.Timed]] for more info on timed `uncons` and `timeout`.
      *
      * As a quick example, let's write a timed pull which emits the
      * string "late!" whenever a chunk of the stream is not emitted
      * within 150 milliseconds:
      *
      * @example {{{
      * scala> import cats.effect.IO
      * scala> import cats.effect.unsafe.implicits.global
      * scala> import scala.concurrent.duration._
      * scala> val s = (Stream("elem") ++ Stream.sleep_[IO](200.millis)).repeat.take(3)
      * scala> s.pull
      *      |  .timed { timedPull =>
      *      |     def go(timedPull: Pull.Timed[IO, String]): Pull[IO, String, Unit] =
      *      |       timedPull.timeout(150.millis) >> // starts new timeout and stops the previous one
      *      |       timedPull.uncons.flatMap {
      *      |         case Some((Right(elems), next)) => Pull.output(elems) >> go(next)
      *      |         case Some((Left(_), next)) => Pull.output1("late!") >> go(next)
      *      |         case None => Pull.done
      *      |       }
      *      |     go(timedPull)
      *      |  }.stream.compile.toVector.unsafeRunSync()
      * res0: Vector[String] = Vector(elem, late!, elem, late!, elem)
      * }}}
      *
      * For a more complex example, look at the implementation of [[Stream.groupWithin]].
      */
    def timed[O2, R](
        pull: Pull.Timed[F, O] => Pull[F, O2, R]
    )(implicit F: Temporal[F]): Pull[F, O2, R] =
      Pull
        .eval(F.unique.mproduct(id => SignallingRef.of(id -> 0.millis)))
        .flatMap { case (initial, time) =>
          def timeouts: Stream[F, Unique.Token] =
            time.discrete
              .dropWhile { case (id, _) => id == initial }
              .switchMap { case (id, duration) =>
                // We cannot move this check into a `filter`:
                // we want `switchMap` to execute and cancel the previous timeout
                if (duration != 0.nanos)
                  Stream.sleep(duration).as(id)
                else
                  Stream.empty
              }

          def output: Stream[F, Either[Unique.Token, Chunk[O]]] =
            timeouts
              .map(_.asLeft)
              .mergeHaltR(self.chunks.map(_.asRight))
              .flatMap {
                case chunk @ Right(_) => Stream.emit(chunk)
                case timeout @ Left(id) =>
                  Stream
                    .eval(time.get)
                    .collect { case (currentTimeout, _) if currentTimeout == id => timeout }
              }

          def toTimedPull(s: Stream[F, Either[Unique.Token, Chunk[O]]]): Pull.Timed[F, O] =
            new Pull.Timed[F, O] {
              type Timeout = Unique.Token

              def uncons: Pull[F, INothing, Option[(Either[Timeout, Chunk[O]], Pull.Timed[F, O])]] =
                s.pull.uncons1
                  .map(_.map { case (r, next) => r -> toTimedPull(next) })

              def timeout(t: FiniteDuration): Pull[F, INothing, Unit] = Pull.eval {
                F.unique.tupleRight(t).flatMap(time.set)
              }
            }

          pull(toTimedPull(output))
        }
  }

  /** Projection of a `Stream` providing various ways to compile a `Stream[F,O]` to a `G[...]`. */
  final class CompileOps[F[_], G[_], O] private[Stream] (
      private val underlying: Pull[F, O, Unit]
  )(implicit compiler: Compiler[F, G]) {

    /** Compiles this stream to a count of the elements in the target effect type `G`.
      */
    def count: G[Long] = foldChunks(0L)((acc, chunk) => acc + chunk.size)

    /** Compiles this stream in to a value of the target effect type `G` and
      * discards any output values of the stream.
      *
      * To access the output values of the stream, use one of the other compilation methods --
      * e.g., [[fold]], [[toVector]], etc.
      */
    def drain: G[Unit] = foldChunks(())((_, _) => ())

    /** Compiles this stream in to a value of the target effect type `G` by folding
      * the output values together, starting with the provided `init` and combining the
      * current value with each output value.
      */
    def fold[B](init: B)(f: (B, O) => B): G[B] =
      foldChunks(init)((acc, c) => c.foldLeft(acc)(f))

    /** Compiles this stream in to a value of the target effect type `G` by folding
      * the output chunks together, starting with the provided `init` and combining the
      * current value with each output chunk.
      *
      * When this method has returned, the stream has not begun execution -- this method simply
      * compiles the stream down to the target effect type.
      */
    def foldChunks[B](init: B)(f: (B, Chunk[O]) => B): G[B] =
      compiler(underlying, init)(f)

    /** Like [[fold]] but uses the implicitly available `Monoid[O]` to combine elements.
      *
      * @example {{{
      * scala> import cats.effect.SyncIO
      * scala> Stream(1, 2, 3, 4, 5).covary[SyncIO].compile.foldMonoid.unsafeRunSync()
      * res0: Int = 15
      * }}}
      */
    def foldMonoid(implicit O: Monoid[O]): G[O] =
      fold(O.empty)(O.combine)

    /** Like [[fold]] but uses the implicitly available `Semigroup[O]` to combine elements.
      * If the stream emits no elements, `None` is returned.
      *
      * @example {{{
      * scala> import cats.effect.SyncIO
      * scala> Stream(1, 2, 3, 4, 5).covary[SyncIO].compile.foldSemigroup.unsafeRunSync()
      * res0: Option[Int] = Some(15)
      * scala> Stream.empty.covaryAll[SyncIO, Int].compile.foldSemigroup.unsafeRunSync()
      * res1: Option[Int] = None
      * }}}
      */
    def foldSemigroup(implicit O: Semigroup[O]): G[Option[O]] =
      fold(Option.empty[O])((acc, o) => acc.map(O.combine(_, o)).orElse(Some(o)))

    /** Compiles this stream in to a value of the target effect type `G`,
      * returning `None` if the stream emitted no values and returning the
      * last value emitted wrapped in `Some` if values were emitted.
      *
      * When this method has returned, the stream has not begun execution -- this method simply
      * compiles the stream down to the target effect type.
      *
      * @example {{{
      * scala> import cats.effect.SyncIO
      * scala> Stream.range(0,100).take(5).covary[SyncIO].compile.last.unsafeRunSync()
      * res0: Option[Int] = Some(4)
      * }}}
      */
    def last: G[Option[O]] =
      foldChunks(Option.empty[O])((acc, c) => c.last.orElse(acc))

    /** Compiles this stream in to a value of the target effect type `G`,
      * raising a `NoSuchElementException` if the stream emitted no values
      * and returning the last value emitted otherwise.
      *
      * When this method has returned, the stream has not begun execution -- this method simply
      * compiles the stream down to the target effect type.
      *
      * @example {{{
      * scala> import cats.effect.SyncIO
      * scala> Stream.range(0,100).take(5).covary[SyncIO].compile.lastOrError.unsafeRunSync()
      * res0: Int = 4
      * scala> Stream.empty.covaryAll[SyncIO, Int].compile.lastOrError.attempt.unsafeRunSync()
      * res1: Either[Throwable, Int] = Left(java.util.NoSuchElementException)
      * }}}
      */
    def lastOrError(implicit G: MonadError[G, Throwable]): G[O] =
      last.flatMap(_.fold(G.raiseError(new NoSuchElementException): G[O])(G.pure))

    /** Gives access to the whole compilation api, where the result is
      * expressed as a `cats.effect.Resource`, instead of bare `G`.
      *
      * {{{
      *  import fs2._
      *  import cats.effect._
      *
      *  val stream = Stream.iterate(0)(_ + 1).take(5).covary[IO]
      *
      *  val s1: Resource[IO, List[Int]] = stream.compile.resource.toList
      *  val s2: Resource[IO, Int] = stream.compile.resource.foldMonoid
      *  val s3: Resource[IO, Option[Int]] = stream.compile.resource.last
      * }}}
      *
      * And so on for every other method in `compile`.
      *
      * The main use case is interacting with Stream methods whose
      * behaviour depends on the Stream lifetime, in cases where you
      * only want to ultimately return a single element.
      *
      * A typical example of this is concurrent combinators, here is
      * an example with `concurrently`:
      *
      * {{{
      * import fs2._
      * import cats.effect._
      * import cats.effect.kernel.Ref
      * import scala.concurrent.duration._
      *
      * trait StopWatch[F[_]] {
      *   def elapsedSeconds: F[Int]
      * }
      * object StopWatch {
      *   def create[F[_]](implicit F: Temporal[F]): Stream[F, StopWatch[F]] =
      *     Stream.eval(F.ref(0)).flatMap { c =>
      *       val api = new StopWatch[F] {
      *         def elapsedSeconds: F[Int] = c.get
      *       }
      *
      *       val process = Stream.fixedRate(1.second).evalMap(_ => c.update(_ + 1))
      *
      *       Stream.emit(api).concurrently(process)
      *   }
      * }
      * }}}
      *
      * This creates a simple abstraction that can be queried by
      * multiple consumers to find out how much time has passed, with
      * a concurrent stream to update it every second.
      *
      * Note that `create` returns a `Stream[F, StopWatch[F]]`, even
      * though there is only one instance being emitted: this is less than ideal,
      *  so we might think about returning an `F[StopWatch[F]]` with the following code
      *
      * {{{
      * StopWatch.create[F].compile.lastOrError
      * }}}
      *
      * but it does not work: the returned `F` terminates the lifetime of the stream,
      * which causes `concurrently` to stop the `process` stream. As a  result, `elapsedSeconds`
      * never gets updated.
      *
      * Alternatively, we could implement `StopWatch` in `F` only
      * using `Fiber.start`, but this is not ideal either:
      * `concurrently` already handles errors, interruption and
      * stopping the producer stream once the consumer lifetime is
      * over, and we don't want to reimplement the machinery for that.
      *
      * So basically what we need is a type that expresses the concept of lifetime,
      * while only ever emitting a single element, which is exactly what `cats.effect.Resource` does.
      *
      * What `compile.resource` provides is the ability to do this:
      *
      * {{{
      * object StopWatch {
      *   // ... def create as before ...
      *
      *   def betterCreate[F[_]: Temporal]: Resource[F, StopWatch[F]] =
      *     create.compile.resource.lastOrError
      * }
      * }}}
      *
      * This works for every other `compile.` method, although it's a
      * very natural fit with `lastOrError`.
      */
    def resource(implicit
        compiler: Compiler[F, Resource[G, *]]
    ): Stream.CompileOps[F, Resource[G, *], O] =
      new Stream.CompileOps[F, Resource[G, *], O](underlying)

    /** Compiles this stream of strings in to a single string.
      * This is more efficient than `foldMonoid` because it uses a `StringBuilder`
      * internally, avoiding intermediate string creation.
      *
      * @example {{{
      * scala> Stream("Hello ", "world!").compile.string
      * res0: String = Hello world!
      * }}}
      */
    def string(implicit ev: O <:< String): G[String] =
      new Stream(underlying).asInstanceOf[Stream[F, String]].compile.to(Collector.string)

    /** Compiles this stream into a value of the target effect type `G` by collecting
      * all of the output values in a collection.
      *
      * Collection building is done via an explicitly passed `Collector`.
      * Standard library collections have collector instances, allowing syntax like:
      * `s.compile.to(List)` or `s.compile.to(Array)` or `s.compile.to(Map)`.
      *
      * A collector is provided for `scodec.bits.ByteVector`, providing efficient byte
      * vector construction from a stream of bytes: `s.compile.to(ByteVector)`.
      *
      * When this method has returned, the stream has not begun execution -- this method simply
      * compiles the stream down to the target effect type.
      *
      * @example {{{
      * scala> import cats.effect.SyncIO
      * scala> val s = Stream.range(0,100).take(5).covary[SyncIO]
      * scala> s.compile.to(List).unsafeRunSync()
      * res0: List[Int] = List(0, 1, 2, 3, 4)
      * scala> s.compile.to(Chunk).unsafeRunSync()
      * res1: Chunk[Int] = Chunk(0, 1, 2, 3, 4)
      * scala> s.map(i => (i % 2, i)).compile.to(Map).unsafeRunSync()
      * res2: Map[Int, Int] = Map(0 -> 4, 1 -> 3)
      * scala> s.map(_.toByte).compile.to(scodec.bits.ByteVector).unsafeRunSync()
      * res3: scodec.bits.ByteVector = ByteVector(5 bytes, 0x0001020304)
      * }}}
      */
    def to(collector: Collector[O]): G[collector.Out] = {
      implicit val G: Monad[G] = compiler.target
      // G.unit suspends creation of the mutable builder
      for {
        _ <- G.unit
        builder <- compiler(underlying, collector.newBuilder) { (acc, c) => acc += c; acc }
      } yield builder.result
    }

    /** Compiles this stream in to a value of the target effect type `G` by logging
      * the output values to a `List`. Equivalent to `to[List]`.
      *
      * When this method has returned, the stream has not begun execution -- this method simply
      * compiles the stream down to the target effect type.
      *
      * @example {{{
      * scala> import cats.effect.SyncIO
      * scala> Stream.range(0,100).take(5).covary[SyncIO].compile.toList.unsafeRunSync()
      * res0: List[Int] = List(0, 1, 2, 3, 4)
      * }}}
      */
    def toList: G[List[O]] = to(List)

    /** Compiles this stream in to a value of the target effect type `G` by logging
      * the output values to a `Vector`. Equivalent to `to[Vector]`.
      *
      * When this method has returned, the stream has not begun execution -- this method simply
      * compiles the stream down to the target effect type.
      *
      * @example {{{
      * scala> import cats.effect.SyncIO
      * scala> Stream.range(0,100).take(5).covary[SyncIO].compile.toVector.unsafeRunSync()
      * res0: Vector[Int] = Vector(0, 1, 2, 3, 4)
      * }}}
      */
    def toVector: G[Vector[O]] = to(Vector)
  }

  /** When merging multiple streams, this represents step of one leg.
    *
    * It is common to `uncons`, however unlike `uncons`, it keeps track
    * of stream scope independently of the main scope of the stream.
    *
    * This assures, that after each next `stepLeg` each Stream `leg` keeps its scope
    * when interpreting.
    *
    * Usual scenarios is to first invoke `stream.pull.stepLeg` and then consume whatever is
    * available in `leg.head`. If the next step is required `leg.stepLeg` will yield next `Leg`.
    *
    * Once the stream will stop to be interleaved (merged), then `stream` allows to return to normal stream
    * invocation.
    */
  final class StepLeg[+F[_], +O](
      val head: Chunk[O],
      private[fs2] val scopeId: Unique.Token,
      private[fs2] val next: Pull[F, O, Unit]
  ) { self =>

    /** Converts this leg back to regular stream. Scope is updated to the scope associated with this leg.
      * Note that when this is invoked, no more interleaving legs are allowed, and this must be very last
      * leg remaining.
      *
      * Note that resulting stream won't contain the `head` of this leg.
      */
    def stream: Stream[F, O] =
      Pull
        .loop[F, O, StepLeg[F, O]](leg => Pull.output(leg.head).flatMap(_ => leg.stepLeg))(
          self.setHead(Chunk.empty)
        )
        .stream

    /** Replaces head of this leg. Useful when the head was not fully consumed. */
    def setHead[O2 >: O](nextHead: Chunk[O2]): StepLeg[F, O2] =
      new StepLeg[F, O2](nextHead, scopeId, next)

    /** Provides an `uncons`-like operation on this leg of the stream. */
    def stepLeg: Pull[F, INothing, Option[StepLeg[F, O]]] =
      Pull.stepLeg(self)
  }

  /**  Implementation for parZip. `AnyVal` classes do not allow inner
    *  classes, so the presence of the `State` trait forces this
    *  method to be outside of the `Stream` class.
    */
  private[fs2] def parZip[F[_], L, R](
      left: Stream[F, L],
      right: Stream[F, R]
  )(implicit F: Concurrent[F]): Stream[F, (L, R)] = {
    sealed trait State
    case object Racing extends State
    case class LeftFirst(leftValue: L, waitOnRight: Deferred[F, Unit]) extends State
    case class RightFirst(rightValue: R, waitOnLeft: Deferred[F, Unit]) extends State

    def emit(l: L, r: R) = Pull.output1(l -> r)

    Stream.eval(F.ref(Racing: State)).flatMap { state =>
      def lhs(stream: Stream[F, L]): Pull[F, (L, R), Unit] =
        stream.pull.uncons1.flatMap {
          case None => Pull.done
          case Some((leftValue, stream)) =>
            Pull.eval {
              F.deferred[Unit].flatMap { awaitRight =>
                state.modify {
                  case Racing =>
                    LeftFirst(leftValue, awaitRight) -> Pull.eval(awaitRight.get)
                  case RightFirst(rightValue, awaitLeft) =>
                    Racing -> { emit(leftValue, rightValue) >> Pull.eval(awaitLeft.complete(())) }
                  case LeftFirst(_, _) => sys.error("fs2 parZip protocol broken by lhs. File a bug")
                }
              }
            }.flatten >> lhs(stream)
        }

      def rhs(stream: Stream[F, R]): Pull[F, (L, R), Unit] =
        stream.pull.uncons1.flatMap {
          case None => Pull.done
          case Some((rightValue, stream)) =>
            Pull.eval {
              F.deferred[Unit].flatMap { awaitLeft =>
                state.modify {
                  case Racing =>
                    RightFirst(rightValue, awaitLeft) -> Pull.eval(awaitLeft.get)
                  case LeftFirst(leftValue, awaitRight) =>
                    Racing -> { emit(leftValue, rightValue) >> Pull.eval(awaitRight.complete(())) }
                  case RightFirst(_, _) =>
                    sys.error("fs2 parZip protocol broken by rhs. File a bug")
                }
              }
            }.flatten >> rhs(stream)
        }

      lhs(left).stream.mergeHaltBoth(rhs(right).stream)
    }
  }

  /** Provides operations on effectful pipes for syntactic convenience. */
  implicit final class PipeOps[F[_], I, O](private val self: Pipe[F, I, O]) extends AnyVal {

    /** Transforms the left input of the given `Pipe2` using a `Pipe`. */
    def attachL[I1, O2](p: Pipe2[F, O, I1, O2]): Pipe2[F, I, I1, O2] =
      (l, r) => p(self(l), r)

    /** Transforms the right input of the given `Pipe2` using a `Pipe`. */
    def attachR[I0, O2](p: Pipe2[F, I0, O, O2]): Pipe2[F, I0, I, O2] =
      (l, r) => p(l, self(r))
  }

  /** Provides operations on pure pipes for syntactic convenience. */
  implicit final class PurePipeOps[I, O](private val self: Pipe[Pure, I, O]) extends AnyVal {

    // This is unsound! See #1838. Left for binary compatibility.
    private[fs2] def covary[F[_]]: Pipe[F, I, O] = self.asInstanceOf[Pipe[F, I, O]]
  }

  /** Provides operations on pure pipes for syntactic convenience. */
  implicit final class PurePipe2Ops[I, I2, O](private val self: Pipe2[Pure, I, I2, O])
      extends AnyVal {

    // This is unsound! See #1838. Left for binary compatibility.
    private[fs2] def covary[F[_]]: Pipe2[F, I, I2, O] = self.asInstanceOf[Pipe2[F, I, I2, O]]
  }

  /** `MonadError` instance for `Stream`.
    *
    * @example {{{
    * scala> import cats.syntax.all._
    * scala> Stream(1, -2, 3).fproduct(_.abs).toList
    * res0: List[(Int, Int)] = List((1,1), (-2,2), (3,3))
    * }}}
    */
  implicit def monadErrorInstance[F[_]](implicit
      ev: ApplicativeError[F, Throwable]
  ): MonadError[Stream[F, *], Throwable] =
    new MonadError[Stream[F, *], Throwable] {
      def pure[A](a: A) = Stream(a)
      def handleErrorWith[A](s: Stream[F, A])(h: Throwable => Stream[F, A]) =
        s.handleErrorWith(h)
      def raiseError[A](t: Throwable) = Stream.raiseError[F](t)
      def flatMap[A, B](s: Stream[F, A])(f: A => Stream[F, B]) = s.flatMap(f)
      def tailRecM[A, B](a: A)(f: A => Stream[F, Either[A, B]]) =
        f(a).flatMap {
          case Left(a)  => tailRecM(a)(f)
          case Right(b) => Stream(b)
        }
    }

  /** `Monoid` instance for `Stream`. */
  implicit def monoidInstance[F[_], O]: Monoid[Stream[F, O]] =
    new Monoid[Stream[F, O]] {
      def empty = Stream.empty
      def combine(x: Stream[F, O], y: Stream[F, O]) = x ++ y
    }

  /** `Align` instance for `Stream`.
    * * @example {{{
    * scala> import cats.syntax.all._
    * scala> Stream(1,2,3).align(Stream("A","B","C","D","E")).toList
    * res0: List[cats.data.Ior[Int,String]] = List(Both(1,A), Both(2,B), Both(3,C), Right(D), Right(E))
    * }}}
    */
  implicit def alignInstance[F[_]]: Align[Stream[F, *]] =
    new Align[Stream[F, *]] {

      private type ZipWithLeft[G[_], O2, L, R] =
        (Chunk[O2], Stream[G, O2]) => Pull[G, Ior[L, R], Unit]

      private def alignWith_[F2[x] >: F[x], O2, O3](
          fa: Stream[F2, O2],
          fb: Stream[F2, O3]
      )(
          k1: ZipWithLeft[F2, O2, O2, O3],
          k2: ZipWithLeft[F2, O3, O2, O3],
          k3: Stream[F2, O3] => Pull[F2, Ior[O2, O3], Unit]
      )(
          f: (O2, O3) => Ior[O2, O3]
      ): Stream[F2, Ior[O2, O3]] = {
        def go(
            leg1: Stream.StepLeg[F2, O2],
            leg2: Stream.StepLeg[F2, O3]
        ): Pull[F2, Ior[O2, O3], Unit] = {
          val l1h = leg1.head
          val l2h = leg2.head
          val out = l1h.zipWith(l2h)(f)
          Pull.output(out) >> {
            if (l1h.size > l2h.size) {
              val extra1 = l1h.drop(l2h.size)
              leg2.stepLeg.flatMap {
                case None      => k1(extra1, leg1.stream)
                case Some(tl2) => go(leg1.setHead(extra1), tl2)
              }
            } else {
              val extra2 = l2h.drop(l1h.size)
              leg1.stepLeg.flatMap {
                case None      => k2(extra2, leg2.stream)
                case Some(tl1) => go(tl1, leg2.setHead(extra2))
              }
            }
          }
        }
        fa.pull.stepLeg.flatMap {
          case Some(leg1) =>
            fb.pull.stepLeg
              .flatMap {
                case Some(leg2) => go(leg1, leg2)
                case None       => k1(leg1.head, leg1.stream)
              }

          case None => k3(fb)
        }.stream
      }

      override def functor: Functor[Stream[F, *]] = Functor[Stream[F, *]]

      override def align[A, B](fa: Stream[F, A], fb: Stream[F, B]): Stream[F, Ior[A, B]] = {

        def echoIor[T, U](s: Stream[F, T], f: T => U) = s.map(f).pull.echo
        def contFor[T, U](chunk: Chunk[T], stream: Stream[F, T], f: T => U) =
          Pull.output(chunk.map(f)) >> echoIor(stream, f)

        alignWith_(fa, fb)(
          contFor(_, _, Ior.left[A, B]),
          contFor(_, _, Ior.right[A, B]),
          echoIor(_, Ior.right[A, B])
        )(Ior.Both[A, B](_, _))
      }
    }

  /** `FunctorFilter` instance for `Stream`.
    *
    * @example {{{
    * scala> import cats.syntax.all._, scala.util._
    * scala> Stream("1", "2", "NaN").mapFilter(s => Try(s.toInt).toOption).toList
    * res0: List[Int] = List(1, 2)
    * }}}
    */
  implicit def functorFilterInstance[F[_]]: FunctorFilter[Stream[F, *]] =
    new FunctorFilter[Stream[F, *]] {
      override def functor: Functor[Stream[F, *]] = Functor[Stream[F, *]]
      override def mapFilter[A, B](fa: Stream[F, A])(f: A => Option[B]): Stream[F, B] = {
        def pull: Stream[F, A] => Pull[F, B, Unit] =
          _.pull.uncons.flatMap {
            case None => Pull.done
            case Some((chunk, rest)) =>
              Pull.output(chunk.mapFilter(f)) >> pull(rest)
          }

        pull(fa).stream
      }
    }

  /** `FunctionK` instance for `F ~> Stream[F, *]`
    *
    * @example {{{
    * scala> import cats.Id
    * scala> Stream.functionKInstance[Id](42).compile.toList
    * res0: cats.Id[List[Int]] = List(42)
    * }}}
    */
  implicit def functionKInstance[F[_]]: F ~> Stream[F, *] =
    new (F ~> Stream[F, *]) {
      def apply[X](fx: F[X]) = Stream.eval(fx)
    }

  implicit def monoidKInstance[F[_]]: MonoidK[Stream[F, *]] =
    new MonoidK[Stream[F, *]] {
      def empty[A]: Stream[F, A] = Stream.empty
      def combineK[A](x: Stream[F, A], y: Stream[F, A]): Stream[F, A] = x ++ y
    }

  /** `Defer` instance for `Stream` */
  implicit def deferInstance[F[_]]: Defer[Stream[F, *]] =
    new Defer[Stream[F, *]] {
      override def defer[A](fa: => Stream[F, A]): Stream[F, A] = Stream.empty ++ fa
    }
}

private[fs2] trait StreamLowPriority {
  implicit def monadInstance[F[_]]: Monad[Stream[F, *]] =
    new Monad[Stream[F, *]] {
      override def pure[A](x: A): Stream[F, A] = Stream.emit(x)

      override def flatMap[A, B](fa: Stream[F, A])(f: A => Stream[F, B]): Stream[F, B] =
        fa.flatMap(f)

      override def tailRecM[A, B](a: A)(f: A => Stream[F, Either[A, B]]): Stream[F, B] =
        f(a).flatMap {
          case Left(a)  => tailRecM(a)(f)
          case Right(b) => Stream(b)
        }
    }
}
