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
package io

import cats.effect.kernel.Async
import cats.effect.kernel.Resource
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.internal.jsdeps.node.bufferMod
import fs2.internal.jsdeps.node.nodeStrings
import fs2.internal.jsdeps.node.streamMod
import fs2.io.internal.ByteChunkOps._
import fs2.io.internal.EventEmitterOps._

import scala.scalajs.js
import scala.scalajs.js.typedarray.Uint8Array
import scala.scalajs.js.|

private[fs2] trait ioplatform {

  def readReadable[F[_]](readable: F[Readable], destroyIfNotEnded: Boolean = true)(implicit
      F: Async[F]
  ): Stream[F, Byte] =
    Stream
      .resource(for {
        readable <- Resource.makeCase(readable.map(_.asInstanceOf[streamMod.Readable])) {
          case (readable, Resource.ExitCase.Succeeded) =>
            F.delay {
              if (!readable.readableEnded & destroyIfNotEnded)
                readable.destroy()
            }
          case (readable, Resource.ExitCase.Errored(ex)) =>
            F.delay(readable.destroy(js.Error(ex.getMessage())))
          case (readable, Resource.ExitCase.Canceled) => F.delay(readable.destroy())
        }
        dispatcher <- Dispatcher[F]
        queue <- Queue.synchronous[F, Unit].toResource
        ended <- F.deferred[Either[Throwable, Unit]].toResource
        _ <- registerListener0(readable, nodeStrings.readable)(_.on_readable(_, _)) { () =>
          dispatcher.unsafeRunAndForget(queue.offer(()))
        }
        _ <- registerListener0(readable, nodeStrings.end)(_.on_end(_, _)) { () =>
          dispatcher.unsafeRunAndForget(ended.complete(Right(())))
        }
        _ <- registerListener[js.Error](readable, nodeStrings.error)(_.on_error(_, _)) { e =>
          dispatcher.unsafeRunAndForget(ended.complete(Left(js.JavaScriptException(e))))
        }
      } yield (readable, queue, ended))
      .flatMap { case (readable, queue, ended) =>
        Stream.fromQueueUnterminated(queue).interruptWhen(ended) >>
          Stream.evalUnChunk(
            F.delay(
              Option(readable.read().asInstanceOf[bufferMod.global.Buffer])
                .fold(Chunk.empty[Byte])(_.toChunk)
            )
          )
      }

  def toReadable[F[_]: Async]: Pipe[F, Byte, Readable] =
    s => Stream.resource(toReadableResource(s))

  def toReadableResource[F[_]](s: Stream[F, Byte])(implicit F: Async[F]): Resource[F, Readable] =
    for {
      dispatcher <- Dispatcher[F]
      semaphore <- Semaphore[F](1).toResource
      ref <- F.ref(s).toResource
      read = semaphore.permit.use { _ =>
        Pull
          .eval(ref.get)
          .flatMap(_.pull.uncons)
          .flatMap {
            case Some((head, tail)) =>
              Pull.eval(ref.set(tail)) >> Pull.output(head)
            case None =>
              Pull.done
          }
          .stream
          .chunks
          .compile
          .last
      }
      readable <- Resource.make {
        F.pure {
          new streamMod.Readable(streamMod.ReadableOptions().setRead { (readable, size) =>
            dispatcher.unsafeRunAndForget(
              read.attempt.flatMap {
                case Left(ex)     => F.delay(readable.destroy(js.Error(ex.getMessage)))
                case Right(chunk) => F.delay(readable.push(chunk.map(_.toUint8Array).orNull)).void
              }
            )
          })
        }
      } { readable =>
        F.delay(if (!readable.readableEnded) readable.destroy())
      }
    } yield readable.asInstanceOf[Readable]

  def writeWritable[F[_]](
      writable: F[Writable],
      endAfterUse: Boolean = true
  )(implicit F: Async[F]): Pipe[F, Byte, INothing] =
    in =>
      Stream.eval(writable.map(_.asInstanceOf[streamMod.Writable])).flatMap { writable =>
        def go(
            s: Stream[F, Byte]
        ): Pull[F, INothing, Unit] = s.pull.uncons.flatMap {
          case Some((head, tail)) =>
            Pull.eval {
              F.async_[Unit] { cb =>
                writable.write(
                  head.toUint8Array: js.Any,
                  e => cb(e.toLeft(()).leftMap(js.JavaScriptException))
                )
              }
            } >> go(tail)
          case None =>
            if (endAfterUse)
              Pull.eval(F.async_[Unit](cb => writable.end(() => cb(Right(())))))
            else
              Pull.done
        }

        go(in).stream.handleErrorWith { ex =>
          Stream.eval(F.delay(writable.destroy(js.Error(ex.getMessage))))
        }.drain
      }

  def readWritable[F[_]: Async](f: Writable => F[Unit]): Stream[F, Byte] =
    Stream.resource(mkWritable).flatMap { case (writable, stream) =>
      stream.concurrently(Stream.eval(f(writable)))
    }

  private def mkWritable[F[_]](implicit F: Async[F]): Resource[F, (Writable, Stream[F, Byte])] =
    for {
      dispatcher <- Dispatcher[F]
      queue <- Queue.synchronous[F, Option[Chunk[Byte]]].toResource
      error <- F.deferred[Throwable].toResource
      writable <- Resource.make {
        F.pure {
          new streamMod.Writable(
            streamMod
              .WritableOptions()
              .setWrite { (writable, chunk, encoding, cb) =>
                dispatcher.unsafeRunAndForget(
                  queue
                    .offer(Some(Chunk.uint8Array(chunk.asInstanceOf[Uint8Array])))
                    .attempt
                    .flatMap(e =>
                      F.delay(
                        cb(
                          e.left.toOption.fold[js.Error | Null](null)(e => js.Error(e.getMessage()))
                        )
                      )
                    )
                )
              }
              .setFinal { (writable, cb) =>
                dispatcher.unsafeRunAndForget(
                  queue
                    .offer(None)
                    .attempt
                    .flatMap(e =>
                      F.delay(
                        cb(
                          e.left.toOption.fold[js.Error | Null](null)(e => js.Error(e.getMessage()))
                        )
                      )
                    )
                )
              }
              .setDestroy { (writable, err, cb) =>
                dispatcher.unsafeRunAndForget {
                  Option(err).fold(F.unit) { err =>
                    error
                      .complete(js.JavaScriptException(err))
                      .attempt
                      .flatMap(e =>
                        F.delay(
                          cb(
                            e.left.toOption
                              .fold[js.Error | Null](null)(e => js.Error(e.getMessage()))
                          )
                        )
                      )
                  }
                }
              }
          )
        }
      } { writable =>
        F.delay(if (!writable.writableEnded) writable.destroy())
      }
      stream = Stream
        .fromQueueNoneTerminatedChunk(queue)
        .concurrently(Stream.eval(error.get.flatMap(F.raiseError[Unit])))
    } yield (writable.asInstanceOf[Writable], stream)

}
