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
package udp

import scala.concurrent.duration.FiniteDuration

import java.net.{
  InetAddress,
  InetSocketAddress,
  NetworkInterface,
  ProtocolFamily,
  StandardSocketOptions
}
import java.nio.channels.{ClosedChannelException, DatagramChannel}

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._

trait SocketGroup[F[_]] {

  /** Provides a UDP Socket that, when run, will bind to the specified address.
    *
    * @param address              address to bind to; defaults to an ephemeral port on all interfaces
    * @param reuseAddress         whether address has to be reused (see `java.net.StandardSocketOptions.SO_REUSEADDR`)
    * @param sendBufferSize       size of send buffer  (see `java.net.StandardSocketOptions.SO_SNDBUF`)
    * @param receiveBufferSize    size of receive buffer (see `java.net.StandardSocketOptions.SO_RCVBUF`)
    * @param allowBroadcast       whether broadcast messages are allowed to be sent; defaults to true
    * @param protocolFamily       protocol family to use when opening the supporting `DatagramChannel`
    * @param multicastInterface   network interface for sending multicast packets
    * @param multicastTTL         time to live of sent multicast packets
    * @param multicastLoopback    whether sent multicast packets should be looped back to this host
    */
  def open(
      address: InetSocketAddress = new InetSocketAddress(0),
      reuseAddress: Boolean = false,
      sendBufferSize: Option[Int] = None,
      receiveBufferSize: Option[Int] = None,
      allowBroadcast: Boolean = true,
      protocolFamily: Option[ProtocolFamily] = None,
      multicastInterface: Option[NetworkInterface] = None,
      multicastTTL: Option[Int] = None,
      multicastLoopback: Boolean = true
  ): Resource[F, Socket[F]]
}

object SocketGroup {
  def forAsync[F[_]: Async]: Resource[F, SocketGroup[F]] =
    AsynchronousSocketGroup[F].map(asg => new AsyncSocketGroup(asg))

  private final class AsyncSocketGroup[F[_]: Async](
      asg: AsynchronousSocketGroup
  ) extends SocketGroup[F] {

    def open(
        address: InetSocketAddress,
        reuseAddress: Boolean,
        sendBufferSize: Option[Int],
        receiveBufferSize: Option[Int],
        allowBroadcast: Boolean,
        protocolFamily: Option[ProtocolFamily],
        multicastInterface: Option[NetworkInterface],
        multicastTTL: Option[Int],
        multicastLoopback: Boolean
    ): Resource[F, Socket[F]] = {
      val mkChannel = Async[F].blocking {
        val channel = protocolFamily
          .map(pf => DatagramChannel.open(pf))
          .getOrElse(DatagramChannel.open())
        channel.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, reuseAddress)
        sendBufferSize.foreach { sz =>
          channel.setOption[Integer](StandardSocketOptions.SO_SNDBUF, sz)
        }
        receiveBufferSize.foreach { sz =>
          channel.setOption[Integer](StandardSocketOptions.SO_RCVBUF, sz)
        }
        channel.setOption[java.lang.Boolean](StandardSocketOptions.SO_BROADCAST, allowBroadcast)
        multicastInterface.foreach { iface =>
          channel.setOption[NetworkInterface](StandardSocketOptions.IP_MULTICAST_IF, iface)
        }
        multicastTTL.foreach { ttl =>
          channel.setOption[Integer](StandardSocketOptions.IP_MULTICAST_TTL, ttl)
        }
        channel
          .setOption[java.lang.Boolean](StandardSocketOptions.IP_MULTICAST_LOOP, multicastLoopback)
        channel.bind(address)
        channel
      }
      Resource(mkChannel.flatMap(ch => mkSocket(ch).map(s => s -> s.close)))
    }

    private def mkSocket(
        channel: DatagramChannel
    ): F[Socket[F]] =
      Async[F].blocking {
        new Socket[F] {
          private val ctx = asg.register(channel)

          def localAddress: F[InetSocketAddress] =
            Async[F].delay(
              Option(channel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress])
                .getOrElse(throw new ClosedChannelException)
            )

          def read(timeout: Option[FiniteDuration]): F[Packet] =
            Async[F].async_[Packet](cb => asg.read(ctx, timeout, result => cb(result)))

          def reads(timeout: Option[FiniteDuration]): Stream[F, Packet] =
            Stream.repeatEval(read(timeout))

          def write(packet: Packet, timeout: Option[FiniteDuration]): F[Unit] =
            Async[F].async_[Unit](cb => asg.write(ctx, packet, timeout, t => cb(t.toLeft(()))))

          def writes(timeout: Option[FiniteDuration]): Pipe[F, Packet, INothing] =
            _.foreach(write(_, timeout))

          def close: F[Unit] = Async[F].blocking(asg.close(ctx))

          def join(group: InetAddress, interface: NetworkInterface): F[AnySourceGroupMembership] =
            Async[F].blocking {
              val membership = channel.join(group, interface)
              new AnySourceGroupMembership {
                def drop = Async[F].blocking(membership.drop)
                def block(source: InetAddress) =
                  Async[F].blocking {
                    membership.block(source); ()
                  }
                def unblock(source: InetAddress) =
                  Async[F].blocking {
                    membership.unblock(source); ()
                  }
                override def toString = "AnySourceGroupMembership"
              }
            }

          def join(
              group: InetAddress,
              interface: NetworkInterface,
              source: InetAddress
          ): F[GroupMembership] =
            Async[F].delay {
              val membership = channel.join(group, interface, source)
              new GroupMembership {
                def drop = Async[F].blocking(membership.drop)
                override def toString = "GroupMembership"
              }
            }

          override def toString =
            s"Socket(${Option(
              channel.socket.getLocalSocketAddress
                .asInstanceOf[InetSocketAddress]
            ).getOrElse("<unbound>")})"
        }
      }
  }
}
