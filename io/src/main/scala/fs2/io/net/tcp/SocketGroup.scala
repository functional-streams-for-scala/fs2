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
package net
package tcp

import java.net.InetSocketAddress
import java.nio.{Buffer, ByteBuffer}
import java.nio.channels.{
  AsynchronousCloseException,
  AsynchronousServerSocketChannel,
  AsynchronousSocketChannel,
  CompletionHandler
}
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.spi.AsynchronousChannelProvider
import java.util.concurrent.ThreadFactory

import cats.syntax.all._
import cats.effect.kernel.{Async, Ref, Resource, Sync}
import cats.effect.std.Semaphore

import com.comcast.ip4s.{Host, IpAddress, Port, SocketAddress}

import fs2.internal.ThreadFactories

/** Resource that provides the ability to open client and server TCP sockets that all share
  * an underlying non-blocking channel group.
  */
trait SocketGroup[F[_]] {

  /** Opens a connection to the specified server represented as a [[Socket]].
    * The connection is closed when the resource is released.
    *
    * @param to      address of remote server
    * @param options socket options to apply to the underlying socket
    */
  def client(
      to: SocketAddress[Host],
      options: List[SocketOption] = List.empty
  ): Resource[F, Socket[F]]

  /** Stream that binds to the specified address and provides a connection for,
    * represented as a [[Socket]], for each client that connects to the bound address.
    *
    * Returns a stream of stream of sockets.
    *
    * The outer stream scopes the lifetime of the server socket.
    * When the outer stream terminates, all open connections will terminate as well.
    * The outer stream emits an element (an inner stream) for each client connection.
    *
    * Each inner stream represents an individual connection, and as such, is a stream
    * that emits a single socket. Failures that occur in an inner stream do *NOT* cause
    * the outer stream to fail.
    *
    * @param address            address to accept connections from; none for all interfaces
    * @param port               port to bind
    * @param options socket options to apply to the underlying socket
    */
  def server(
      address: Option[Host] = None,
      port: Option[Port] = None,
      options: List[SocketOption] = List.empty
  ): Stream[F, Resource[F, Socket[F]]]

  /** Like [[server]] but provides the `SocketAddress` of the bound server socket before providing accepted sockets.
    * The inner stream emits one socket for each client that connects to the server.
    */
  def serverResource(
      address: Option[Host] = None,
      port: Option[Port] = None,
      options: List[SocketOption] = List.empty
  ): Resource[F, (SocketAddress[IpAddress], Stream[F, Resource[F, Socket[F]]])]
}

object SocketGroup {

  /** Creates a `SocketGroup`.
    *
    * All reads and writes are performed on a non-blocking thread pool
    * associated with the `SocketGroup`. The non-blocking thread pool is sized to
    * the number of available processors if nonBlockingThreadCount is <= 0.
    * See https://openjdk.java.net/projects/nio/resources/AsynchronousIo.html for more
    * information on NIO thread pooling.
    */
  def forAsync[F[_]: Async](
      nonBlockingThreadCount: Int = 0,
      nonBlockingThreadFactory: ThreadFactory =
        ThreadFactories.named("fs2-socket-group-non-blocking", true)
  ): Resource[F, SocketGroup[F]] =
    Resource(Sync[F].blocking {
      val threadCount =
        if (nonBlockingThreadCount <= 0) Runtime.getRuntime.availableProcessors
        else nonBlockingThreadCount
      val acg = AsynchronousChannelGroup.withFixedThreadPool(threadCount, nonBlockingThreadFactory)
      val group = new AsyncSocketGroup(acg)
      (group, Sync[F].blocking(acg.shutdown()))
    })

  private final class AsyncSocketGroup[F[_]: Async](channelGroup: AsynchronousChannelGroup)
      extends SocketGroup[F] {

    def client(
        to: SocketAddress[Host],
        options: List[SocketOption]
    ): Resource[F, Socket[F]] = {
      def setup: F[AsynchronousSocketChannel] =
        Async[F].blocking {
          val ch =
            AsynchronousChannelProvider.provider().openAsynchronousSocketChannel(channelGroup)
          options.foreach { opt =>
            ch.setOption[opt.Value](opt.key, opt.value)
          }
          ch
        }

      def connect(ch: AsynchronousSocketChannel): F[AsynchronousSocketChannel] =
        to.resolve[F].flatMap { ip =>
          Async[F].async_[AsynchronousSocketChannel] { cb =>
            ch.connect(
              ip.toInetSocketAddress,
              null,
              new CompletionHandler[Void, Void] {
                def completed(result: Void, attachment: Void): Unit =
                  cb(Right(ch))
                def failed(rsn: Throwable, attachment: Void): Unit =
                  cb(Left(rsn))
              }
            )
          }
        }

      Resource.eval(setup.flatMap(connect)).flatMap(apply(_))
    }

    def server(
        address: Option[Host],
        port: Option[Port],
        options: List[SocketOption]
    ): Stream[F, Resource[F, Socket[F]]] =
      Stream
        .resource(
          serverResource(
            address,
            port,
            options
          )
        )
        .flatMap { case (_, clients) => clients }

    def serverResource(
        address: Option[Host],
        port: Option[Port],
        options: List[SocketOption]
    ): Resource[F, (SocketAddress[IpAddress], Stream[F, Resource[F, Socket[F]]])] = {

      val setup: F[AsynchronousServerSocketChannel] =
        address.traverse(_.resolve[F]).flatMap { addr =>
          Async[F].blocking {
            val ch = AsynchronousChannelProvider
              .provider()
              .openAsynchronousServerSocketChannel(channelGroup)
            options.foreach { opt =>
              ch.setOption[opt.Value](opt.key, opt.value)
            }
            ch.bind(
              new InetSocketAddress(
                addr.map(_.toInetAddress).orNull,
                port.map(_.value).getOrElse(0)
              )
            )
            ch
          }
        }

      def cleanup(sch: AsynchronousServerSocketChannel): F[Unit] =
        Async[F].blocking(if (sch.isOpen) sch.close())

      def acceptIncoming(
          sch: AsynchronousServerSocketChannel
      ): Stream[F, Resource[F, Socket[F]]] = {
        def go: Stream[F, Resource[F, Socket[F]]] = {
          def acceptChannel: F[AsynchronousSocketChannel] =
            Async[F].async_[AsynchronousSocketChannel] { cb =>
              sch.accept(
                null,
                new CompletionHandler[AsynchronousSocketChannel, Void] {
                  def completed(ch: AsynchronousSocketChannel, attachment: Void): Unit =
                    cb(Right(ch))
                  def failed(rsn: Throwable, attachment: Void): Unit =
                    cb(Left(rsn))
                }
              )
            }

          Stream.eval(acceptChannel.attempt).flatMap {
            case Left(_)         => Stream.empty[F]
            case Right(accepted) => Stream.emit(apply(accepted))
          } ++ go
        }

        go.handleErrorWith {
          case err: AsynchronousCloseException =>
            Stream.eval(Async[F].blocking(sch.isOpen)).flatMap { isOpen =>
              if (isOpen) Stream.raiseError[F](err)
              else Stream.empty
            }
          case err => Stream.raiseError[F](err)
        }
      }

      Resource.make(setup)(cleanup).map { sch =>
        val jLocalAddress = sch.getLocalAddress.asInstanceOf[java.net.InetSocketAddress]
        val localAddress = SocketAddress.fromInetSocketAddress(jLocalAddress)
        (localAddress, acceptIncoming(sch))
      }
    }

    private def apply(
        ch: AsynchronousSocketChannel
    ): Resource[F, Socket[F]] = {
      val socket = (Semaphore[F](1), Semaphore[F](1), Ref[F].of(ByteBuffer.allocate(0))).mapN {
        (readSemaphore, writeSemaphore, bufferRef) =>
          // Reads data to remaining capacity of supplied ByteBuffer
          def readChunk(buff: ByteBuffer): F[Int] =
            Async[F].async_[Int] { cb =>
              ch.read(
                buff,
                (),
                new CompletionHandler[Integer, Unit] {
                  def completed(result: Integer, attachment: Unit): Unit =
                    cb(Right(result))
                  def failed(err: Throwable, attachment: Unit): Unit =
                    cb(Left(err))
                }
              )
            }

          // gets buffer of desired capacity, ready for the first read operation
          // If the buffer does not have desired capacity it is resized (recreated)
          // buffer is also reset to be ready to be written into.
          def getBufferOf(sz: Int): F[ByteBuffer] =
            bufferRef.get.flatMap { buff =>
              if (buff.capacity() < sz)
                Async[F].delay(ByteBuffer.allocate(sz)).flatTap(bufferRef.set)
              else
                Async[F].delay {
                  (buff: Buffer).clear()
                  (buff: Buffer).limit(sz)
                  buff
                }
            }

          // When the read operation is done, this will read up to buffer's position bytes from the buffer
          // this expects the buffer's position to be at bytes read + 1
          def releaseBuffer(buff: ByteBuffer): F[Chunk[Byte]] =
            Async[F].delay {
              val read = buff.position()
              val result =
                if (read == 0) Chunk.empty
                else {
                  val dest = new Array[Byte](read)
                  (buff: Buffer).flip()
                  buff.get(dest)
                  Chunk.array(dest)
                }
              (buff: Buffer).clear()
              result
            }

          def read0(max: Int): F[Option[Chunk[Byte]]] =
            readSemaphore.permit.use { _ =>
              getBufferOf(max).flatMap { buff =>
                readChunk(buff).flatMap { read =>
                  if (read < 0) Async[F].pure(None)
                  else releaseBuffer(buff).map(Some(_))
                }
              }
            }

          def readN0(max: Int): F[Option[Chunk[Byte]]] =
            readSemaphore.permit.use { _ =>
              getBufferOf(max).flatMap { buff =>
                def go: F[Option[Chunk[Byte]]] =
                  readChunk(buff).flatMap { readBytes =>
                    if (readBytes < 0 || buff.position() >= max)
                      // read is done
                      releaseBuffer(buff).map(Some(_))
                    else go
                  }
                go
              }
            }

          def write0(bytes: Chunk[Byte]): F[Unit] = {
            def go(buff: ByteBuffer): F[Unit] =
              Async[F]
                .async_[Unit] { cb =>
                  ch.write(
                    buff,
                    (),
                    new CompletionHandler[Integer, Unit] {
                      def completed(result: Integer, attachment: Unit): Unit =
                        cb(Right(()))
                      def failed(err: Throwable, attachment: Unit): Unit =
                        cb(Left(err))
                    }
                  )
                }
            writeSemaphore.permit.use { _ =>
              go(bytes.toByteBuffer)
            }
          }

          ///////////////////////////////////
          ///////////////////////////////////

          new Socket[F] {
            def readN(numBytes: Int): F[Option[Chunk[Byte]]] =
              readN0(numBytes)
            def read(maxBytes: Int): F[Option[Chunk[Byte]]] =
              read0(maxBytes)
            def reads(maxBytes: Int): Stream[F, Byte] =
              Stream.eval(read(maxBytes)).flatMap {
                case Some(bytes) =>
                  Stream.chunk(bytes) ++ reads(maxBytes)
                case None => Stream.empty
              }

            def write(bytes: Chunk[Byte]): F[Unit] =
              write0(bytes)
            def writes: Pipe[F, Byte, INothing] =
              _.chunks.foreach(write)

            def localAddress: F[SocketAddress[IpAddress]] =
              Async[F].blocking(
                SocketAddress.fromInetSocketAddress(
                  ch.getLocalAddress.asInstanceOf[InetSocketAddress]
                )
              )
            def remoteAddress: F[SocketAddress[IpAddress]] =
              Async[F].blocking(
                SocketAddress.fromInetSocketAddress(
                  ch.getRemoteAddress.asInstanceOf[InetSocketAddress]
                )
              )
            def isOpen: F[Boolean] = Async[F].blocking(ch.isOpen)
            def close: F[Unit] = Async[F].blocking(ch.close())
            def endOfOutput: F[Unit] =
              Async[F].blocking {
                ch.shutdownOutput(); ()
              }
            def endOfInput: F[Unit] =
              Async[F].blocking {
                ch.shutdownInput(); ()
              }
          }
      }
      Resource.make(socket)(_ => Async[F].blocking(if (ch.isOpen) ch.close else ()).attempt.void)
    }
  }
}