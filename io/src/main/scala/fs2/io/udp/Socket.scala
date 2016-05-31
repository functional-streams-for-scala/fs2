package fs2.io.udp

import java.net.{InetAddress,NetworkInterface,InetSocketAddress}
import java.nio.channels.{ClosedChannelException,DatagramChannel}

import fs2._

sealed trait Socket[F[_]] {

  /**
   * Reads a single packet from this udp socket.
   */
  def read: F[Packet]

  /**
   * Reads packets received from this udp socket.
   *
   * Note that multiple `reads` may execute at same time, causing each evaluation to receive fair
   * amount of messages.
   *
   * @return stream of packets
   */
  def reads: Stream[F,Packet]

  /**
   * Write a single packet to this udp socket.
   *
   * @param packet  Packet to write
   */
  def write(packet: Packet): F[Unit]

  /**
   * Writes supplied packets to this udp socket.
   */
  def writes: Sink[F,Packet]

  /** Returns the local address of this udp socket. */
  def localAddress: F[InetSocketAddress]

  /** Closes this socket. */
  def close: F[Unit]

  /**
   * Joins a multicast group on a specific network interface.
   *
   * @param group address of group to join
   * @param interface network interface upon which to listen for datagrams
   */
  def join(group: InetAddress, interface: NetworkInterface): F[AnySourceGroupMembership]

  /**
   * Joins a source specific multicast group on a specific network interface.
   *
   * @param group address of group to join
   * @param interface network interface upon which to listen for datagrams
   * @param source limits received packets to those sent by the source
   */
  def join(group: InetAddress, interface: NetworkInterface, source: InetAddress): F[GroupMembership]

  /** Result of joining a multicast group on a UDP socket. */
  sealed trait GroupMembership {
    /** Leaves the multicast group, resulting in no further packets from this group being read. */
    def drop: F[Unit]
  }

  /** Result of joining an any-source multicast group on a UDP socket. */
  sealed trait AnySourceGroupMembership extends GroupMembership {

    /** Blocks packets from the specified source address. */
    def block(source: InetAddress): F[Unit]

    /** Unblocks packets from the specified source address. */
    def unblock(source: InetAddress): F[Unit]
  }
}


object Socket {

  private[fs2] def mkSocket[F[_]](channel: DatagramChannel)(implicit AG: AsynchronousSocketGroup, F: Async[F], FR: Async.Run[F]): F[Socket[F]] = F.delay {
    new Socket[F] {
      private val ctx = AG.register(channel)

      private def invoke(f: => Unit): Unit =
        FR.unsafeRunAsyncEffects(F.delay(f))(_ => ())

      def localAddress: F[InetSocketAddress] =
        F.delay(Option(channel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress]).getOrElse(throw new ClosedChannelException))

      def read: F[Packet] = F.async(cb => F.delay { AG.read(ctx, result => invoke(cb(result))) })

      def reads: Stream[F, Packet] =
        Stream.repeatEval(read)

      def write(packet: Packet): F[Unit] =
        F.async(cb => F.delay {
          AG.write(ctx, packet, _ match { case Some(t) => invoke(cb(Left(t))); case None => invoke(cb(Right(()))) })
        })

      def writes: Sink[F, Packet] =
        _.flatMap(p => Stream.eval(write(p)))

      def close: F[Unit] = F.delay { AG.close(ctx) }

      def join(group: InetAddress, interface: NetworkInterface): F[AnySourceGroupMembership] = F.delay {
        val membership = channel.join(group, interface)
        new AnySourceGroupMembership {
          def drop = F.delay { membership.drop }
          def block(source: InetAddress) = F.delay { membership.block(source); () }
          def unblock(source: InetAddress) = F.delay { membership.unblock(source); () }
          override def toString = "AnySourceGroupMembership"
        }
      }

      def join(group: InetAddress, interface: NetworkInterface, source: InetAddress): F[GroupMembership] = F.delay {
        val membership = channel.join(group, interface, source)
        new GroupMembership {
          def drop = F.delay { membership.drop }
          override def toString = "GroupMembership"
        }
      }

      override def toString = s"Socket(${Option(channel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress]).getOrElse("<unbound>")})"
    }
  }
}

