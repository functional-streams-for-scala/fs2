package fs2
package io

import java.io.{IOException, InputStream}

import cats.~>
import cats.implicits._
import cats.effect.{Concurrent, ExitCase, IO}
import cats.effect.implicits._

import fs2.Chunk.Bytes
import fs2.concurrent.{Queue, SignallingRef}

private[io] object JavaInputOutputStream {

  /** state of the upstream, we only indicate whether upstream is done and if it failed **/
  private final case class UpStreamState(done: Boolean, err: Option[Throwable])
  private sealed trait DownStreamState { self =>
    def isDone: Boolean = self match {
      case Done(_) => true
      case _       => false
    }
  }
  private final case class Done(rslt: Option[Throwable]) extends DownStreamState
  private final case class Ready(rem: Option[Bytes]) extends DownStreamState

  def toInputStream[F[_]](implicit F: Concurrent[F],
                          ioConcurrent: Concurrent[IO]): Pipe[F, Byte, InputStream] = {

    def markUpstreamDone(queue: Queue[IO, Either[Option[Throwable], Bytes]],
                         upState: SignallingRef[IO, UpStreamState],
                         result: Option[Throwable]): F[Unit] =
      F.liftIO(
        upState.set(UpStreamState(done = true, err = result)) *> queue.enqueue1(Left(result)))

    /**
      * Takes source and runs it through queue, interrupting when dnState signals stream is done.
      * Note when the exception in stream is encountered the exception is emitted on the left to the queue
      * and that would be the last message enqueued.
      *
      * Emits only once, but runs in background until either source is exhausted or `interruptWhenTrue` yields to true
      */
    def processInput(
        source: Stream[F, Byte],
        queue: Queue[IO, Either[Option[Throwable], Bytes]],
        upState: SignallingRef[IO, UpStreamState],
        dnState: SignallingRef[IO, DownStreamState]
    ): Stream[F, Unit] =
      Stream
        .eval(
          source.chunks
            .evalMap(ch => F.liftIO(queue.enqueue1(Right(ch.toBytes))))
            .interruptWhen(
              dnState.discrete
                .map(_.isDone)
                .filter(identity)
                .translate(new (IO ~> F) { def apply[X](ix: IO[X]) = F.liftIO(ix) }))
            .compile
            .drain
            .guaranteeCase {
              case ExitCase.Completed => markUpstreamDone(queue, upState, None)
              case ExitCase.Error(t)  => markUpstreamDone(queue, upState, Some(t))
              case ExitCase.Canceled  => markUpstreamDone(queue, upState, None)
            }
            .start
        )
        .void

    /**
      * Closes the stream if not closed yet.
      * If the stream is closed, this will return once the upstream stream finishes its work and
      * releases any resources that upstream may hold.
      */
    def closeIs(
        upState: SignallingRef[IO, UpStreamState],
        dnState: SignallingRef[IO, DownStreamState]
    ): Unit =
      close(upState, dnState).unsafeRunSync

    /**
      * Reads single chunk of bytes of size `len` into array b.
      *
      * This is implementation of InputStream#read.
      *
      * Inherently this method will block until data from the queue are available
      */
    def readIs(
        dest: Array[Byte],
        off: Int,
        len: Int,
        queue: Queue[IO, Either[Option[Throwable], Bytes]],
        dnState: SignallingRef[IO, DownStreamState]
    ): Int =
      readOnce(dest, off, len, queue, dnState).unsafeRunSync

    /**
      * Reads single int value
      *
      * This is implementation of InputStream#read.
      *
      * Inherently this method will block until data from the queue are available
      *
      *
      */
    def readIs1(
        queue: Queue[IO, Either[Option[Throwable], Bytes]],
        dnState: SignallingRef[IO, DownStreamState]
    ): Int = {

      def go(acc: Array[Byte]): IO[Int] =
        readOnce(acc, 0, 1, queue, dnState).flatMap { read =>
          if (read < 0) IO.pure(-1)
          else if (read == 0) go(acc)
          else IO.pure(acc(0) & 0xFF)
        }

      go(new Array[Byte](1)).unsafeRunSync
    }

    def readOnce(
        dest: Array[Byte],
        off: Int,
        len: Int,
        queue: Queue[IO, Either[Option[Throwable], Bytes]],
        dnState: SignallingRef[IO, DownStreamState]
    ): IO[Int] = {
      // in case current state has any data available from previous read
      // this will cause the data to be acquired, state modified and chunk returned
      // won't modify state if the data cannot be acquired
      def tryGetChunk(s: DownStreamState): (DownStreamState, Option[Bytes]) =
        s match {
          case Done(None)      => s -> None
          case Done(Some(err)) => s -> None
          case Ready(None)     => s -> None
          case Ready(Some(bytes)) =>
            val cloned = Chunk.Bytes(bytes.toArray)
            if (bytes.size <= len) Ready(None) -> Some(cloned)
            else {
              val (out, rem) = cloned.splitAt(len)
              Ready(Some(rem.toBytes)) -> Some(out.toBytes)
            }
        }

      def setDone(rsn: Option[Throwable])(s0: DownStreamState): DownStreamState = s0 match {
        case s @ Done(_) => s
        case _           => Done(rsn)
      }

      dnState.modify { s =>
        val (n, out) = tryGetChunk(s)

        val result = out match {
          case Some(bytes) =>
            IO.delay {
              Array.copy(bytes.values, 0, dest, off, bytes.size)
              bytes.size
            }
          case None =>
            n match {
              case Done(None) => (-1).pure[IO]
              case Done(Some(err)) =>
                IO.raiseError[Int](new IOException("Stream is in failed state", err))
              case _ =>
                // Ready is guaranteed at this time to be empty
                queue.dequeue1.flatMap {
                  case Left(None) =>
                    dnState
                      .update(setDone(None))
                      .as(-1) // update we are done, next read won't succeed
                  case Left(Some(err)) => // update we are failed, next read won't succeed
                    dnState.update(setDone(err.some)) >> IO.raiseError[Int](
                      new IOException("UpStream failed", err))
                  case Right(bytes) =>
                    val (copy, maybeKeep) =
                      if (bytes.size <= len) bytes -> None
                      else {
                        val (out, rem) = bytes.splitAt(len)
                        out.toBytes -> rem.toBytes.some
                      }
                    IO.delay {
                      Array.copy(copy.values, 0, dest, off, copy.size)
                    } >> (maybeKeep match {
                      case Some(rem) if rem.size > 0 =>
                        dnState.set(Ready(rem.some)).as(copy.size)
                      case _ => copy.size.pure[IO]
                    })
                }
            }
        }

        n -> result
      }.flatten
    }

    /**
      * Closes input stream and awaits completion of the upstream
      */
    def close(
        upState: SignallingRef[IO, UpStreamState],
        dnState: SignallingRef[IO, DownStreamState]
    ): IO[Unit] =
      dnState.update {
        case s @ Done(_) => s
        case other       => Done(None)
      } >>
        upState.discrete
          .collectFirst {
            case UpStreamState(true, maybeErr) =>
              maybeErr // await upStreamDome to yield as true
          }
          .compile
          .last
          .flatMap {
            _.flatten match {
              case None      => IO.pure(())
              case Some(err) => IO.raiseError[Unit](err)
            }
          }

    /*
     * Implementation note:
     *
     * We run this through 3 synchronous primitives
     *
     * - Synchronous Queue -  used to signal next available chunk, or when the upstream is done/failed
     * - UpStream signal -    used to monitor state of upstream, primarily to indicate to `close`
     *                        that upstream has finished and is safe time to terminate
     * - DownStream signal -  keeps any remainders from last `read` and signals
     *                        that downstream has been terminated that in turn kills upstream
     */
    (source: Stream[F, Byte]) =>
      Stream
        .eval(
          F.liftIO(
            (
              Queue.synchronous[IO, Either[Option[Throwable], Bytes]],
              SignallingRef[IO, UpStreamState](UpStreamState(done = false, err = None)),
              SignallingRef[IO, DownStreamState](Ready(None))
            ).tupled))
        .flatMap {
          case (queue, upState, dnState) =>
            processInput(source, queue, upState, dnState)
              .as(
                new InputStream {
                  override def close(): Unit = closeIs(upState, dnState)
                  override def read(b: Array[Byte], off: Int, len: Int): Int =
                    readIs(b, off, len, queue, dnState)
                  def read(): Int = readIs1(queue, dnState)
                }
              )
              .onFinalize(F.liftIO(close(upState, dnState)))
        }
  }
}
