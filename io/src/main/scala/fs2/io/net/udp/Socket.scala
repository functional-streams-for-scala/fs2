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
package udp

import java.net.NetworkInterface

import com.comcast.ip4s._

/** Provides the ability to read/write from a UDP socket in the effect `F`.
  */
trait Socket[F[_]] {

  /** Reads a single packet from this udp socket.
    */
  def read: F[Packet]

  /** Reads packets received from this udp socket.
    *
    * Note that multiple `reads` may execute at same time, causing each evaluation to receive fair
    * amount of messages.
    *
    * @return stream of packets
    */
  def reads: Stream[F, Packet]

  /** Write a single packet to this udp socket.
    *
    * @param packet  Packet to write
    */
  def write(packet: Packet): F[Unit]

  /** Writes supplied packets to this udp socket.
    */
  def writes: Pipe[F, Packet, INothing]

  /** Returns the local address of this udp socket. */
  def localAddress: F[SocketAddress[IpAddress]]

  /** Closes this socket. */
  def close: F[Unit]

  /** Joins a multicast group on a specific network interface.
    *
    * @param join group to join
    * @param interface network interface upon which to listen for datagrams
    */
  def join(join: MulticastJoin[IpAddress], interface: NetworkInterface): F[GroupMembership]

  /** Result of joining a multicast group on a UDP socket. */
  trait GroupMembership {

    /** Leaves the multicast group, resulting in no further packets from this group being read. */
    def drop: F[Unit]

    /** Blocks packets from the specified source address. */
    def block(source: IpAddress): F[Unit]

    /** Unblocks packets from the specified source address. */
    def unblock(source: IpAddress): F[Unit]
  }
}