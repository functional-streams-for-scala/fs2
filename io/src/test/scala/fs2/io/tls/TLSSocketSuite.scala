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
package tls

import scala.concurrent.duration._

import java.net.{InetAddress, InetSocketAddress}
import javax.net.ssl.{SNIHostName, SSLContext}

import cats.effect.{IO, Resource}
import cats.syntax.all._

class TLSSocketSuite extends TLSSuite {
  val size = 8192

  group("TLSSocket") {
    group("google") {
      def googleSetup(protocol: String) =
        for {
          tlsContext <- Resource.eval(Network[IO].tlsContext.system)
          socketGroup <- Network[IO].tcpSocketGroup
          socket <- socketGroup.client(new InetSocketAddress("google.com", 443))
          tlsSocket <- tlsContext.client(
            socket,
            TLSParameters(
              serverNames = List(new SNIHostName("www.google.com")).some,
              protocols = List(protocol).some
            )
          )
        } yield tlsSocket

      val googleDotCom = "GET / HTTP/1.1\r\nHost: www.google.com\r\n\r\n"
      val httpOk = "HTTP/1.1 200 OK"

      def writesBeforeReading(protocol: String) =
        test(s"$protocol - client writes before reading") {
          Stream
            .resource(googleSetup(protocol))
            .flatMap { tlsSocket =>
              Stream(googleDotCom)
                .covary[IO]
                .through(text.utf8Encode)
                .through(tlsSocket.writes()) ++
                tlsSocket
                  .reads(size)
                  .through(text.utf8Decode)
                  .through(text.lines)
            }
            .head
            .compile
            .string
            .assertEquals(httpOk)
        }

      def readsBeforeWriting(protocol: String) =
        test(s"$protocol - client reads before writing") {
          Stream
            .resource(googleSetup(protocol))
            .flatMap { socket =>
              val send = Stream(googleDotCom)
                .through(text.utf8Encode)
                .through(socket.writes())
              val receive = socket
                .reads(size)
                .through(text.utf8Decode)
                .through(text.lines)

              receive.concurrently(send.delayBy(100.millis))
            }
            .head
            .compile
            .string
            .assertEquals(httpOk)
        }

      List("TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3").foreach { protocol =>
        val supportedByPlatform: Boolean =
          Either
            .catchNonFatal(SSLContext.getInstance(protocol))
            .isRight

        if (!supportedByPlatform)
          test(s"$protocol - not supported by this platform".ignore) {}
        else {
          writesBeforeReading(protocol)
          readsBeforeWriting(protocol)
        }
      }
    }

    test("echo") {
      val msg = Chunk.array(("Hello, world! " * 20000).getBytes)

      val setup = for {
        socketGroup <- Network[IO].tcpSocketGroup
        tlsContext <- Resource.eval(testTlsContext)
        inetAddress = new InetSocketAddress(InetAddress.getByName(null), 0)
        addressAndConnections <- socketGroup.serverResource(inetAddress)
        (serverAddress, connections) = addressAndConnections
        server = connections.flatMap(c => Stream.resource(c.flatMap(tlsContext.server(_))))
        client <- socketGroup.client(serverAddress).flatMap(tlsContext.client(_))
      } yield server -> client

      Stream
        .resource(setup)
        .flatMap { case (server, clientSocket) =>
          val echoServer = server.map { socket =>
            socket.reads(size).chunks.foreach(socket.write(_))
          }.parJoinUnbounded

          val client =
            Stream.exec(clientSocket.write(msg)) ++
              clientSocket.reads(size).take(msg.size.toLong)

          client.concurrently(echoServer)
        }
        .compile
        .to(Chunk)
        .assertEquals(msg)
    }
  }
}
