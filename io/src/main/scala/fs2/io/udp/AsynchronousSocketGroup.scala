package fs2
package io
package udp

import scala.collection.mutable.{Queue=>MutableQueue}

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{CancelledKeyException,ClosedChannelException,DatagramChannel,Selector,SelectionKey}
import java.util.concurrent.locks.ReentrantLock

/**
 * Supports read/write operations on an arbitrary number of UDP sockets using a shared selector thread.
 *
 * Each `AsynchronousSocketGroup` is assigned a single daemon thread that performs all read/write operations.
 */
sealed trait AsynchronousSocketGroup {
  private[udp] type Context
  private[udp] def register(channel: DatagramChannel): Context
  private[udp] def read(ctx: Context, cb: Either[Throwable,Packet] => Unit): Unit
  private[udp] def write(ctx: Context, packet: Packet, cb: Option[Throwable] => Unit): Unit
  private[udp] def close(ctx: Context): Unit

  /**
   * Shuts down the daemon thread used for selection and rejects all future read/write requests.
   */
  def close: Unit
}

object AsynchronousSocketGroup {

  def apply(): AsynchronousSocketGroup = new AsynchronousSocketGroup {

    class Attachment(
      readers: MutableQueue[Either[Throwable,Packet] => Unit] = MutableQueue(),
      writers: MutableQueue[(Packet,Option[Throwable] => Unit)] = MutableQueue()
    ) {

      def dequeueReader: Option[Either[Throwable,Packet] => Unit] = {
        if (readers.isEmpty) None
        else Some(readers.dequeue())
      }

      def queueReader(reader: Either[Throwable,Packet] => Unit): Unit = {
        if (closed) reader(Left(new ClosedChannelException))
        else {
          readers += reader
          ()
        }
      }

      def hasReaders: Boolean = readers.nonEmpty

      def peekWriter: Option[(Packet,Option[Throwable] => Unit)] = {
        if (writers.isEmpty) None
        else Some(writers.head)
      }

      def dequeueWriter: Option[(Packet,Option[Throwable] => Unit)] = {
        if (writers.isEmpty) None
        else Some(writers.dequeue())
      }

      def queueWriter(writer: (Packet,Option[Throwable] => Unit)): Unit = {
        if (closed) writer._2(Some(new ClosedChannelException))
        else {
          writers += writer
          ()
        }
      }

      def hasWriters: Boolean = writers.nonEmpty

      def close: Unit = {
        readers.foreach { cb => cb(Left(new ClosedChannelException)) }
        readers.clear
        writers.foreach { case (_, cb) => cb(Some(new ClosedChannelException)) }
        writers.clear
      }
    }

    type Context = (DatagramChannel, SelectionKey, Attachment)

    private val selectorLock = new ReentrantLock()
    private val selector = Selector.open()
    private val closeLock = new Object
    @volatile private var closed = false
    private val pendingThunks: MutableQueue[() => Unit] = MutableQueue()

    override def register(channel: DatagramChannel): Context = {
      channel.configureBlocking(false)
      val attachment = new Attachment()
      val key = {
        selectorLock.lock
        try {
          selector.wakeup
          channel.register(selector, 0, attachment)
        } finally {
          selectorLock.unlock
        }
      }
      (channel, key, attachment)
    }

    override def read(ctx: Context, cb: Either[Throwable,Packet] => Unit): Unit = {
      onSelectorThread {
        ctx._3.queueReader(cb)
        try ctx._2.interestOps(ctx._2.interestOps | SelectionKey.OP_READ)
        catch {
          case t: CancelledKeyException => // Ignore; key was closed
        }
        ()
      } { cb(Left(new ClosedChannelException)) }
    }

    override def write(ctx: Context, packet: Packet, cb: Option[Throwable] => Unit): Unit = {
      onSelectorThread {
        ctx._3.queueWriter((packet, cb))
        try ctx._2.interestOps(ctx._2.interestOps | SelectionKey.OP_WRITE)
        catch {
          case t: CancelledKeyException => // Ignore; key was closed
        }
        ()
      } { cb(Some(new ClosedChannelException)) }
    }

    override def close(ctx: Context): Unit = {
      onSelectorThread {
        ctx._1.close
        ctx._3.close
      } { () }
    }

    override def close: Unit = {
      closeLock.synchronized { closed = true }
    }

    private def onSelectorThread(f: => Unit)(ifClosed: => Unit): Unit = {
      closeLock.synchronized {
        if (closed) {
          ifClosed
        } else {
          pendingThunks.synchronized {
            pendingThunks.+=(() => f)
          }
          selector.wakeup()
          ()
        }
      }
    }

    private def read1(key: SelectionKey, channel: DatagramChannel, attachment: Attachment, readBuffer: ByteBuffer): Unit = {
      readBuffer.clear
      val src = channel.receive(readBuffer).asInstanceOf[InetSocketAddress]
      if (src ne null) {
        attachment.dequeueReader match {
          case Some(reader) =>
            readBuffer.flip
            val bytes = Array.ofDim[Byte](readBuffer.remaining)
            readBuffer.get(bytes)
            readBuffer.clear
            reader(Right(new Packet(src, Chunk.bytes(bytes))))
            if (!attachment.hasReaders) {
              key.interestOps(key.interestOps & ~SelectionKey.OP_READ)
              ()
            }
          case None =>
            sys.error("key marked for read but no reader")
        }
      }
    }

    private def write1(key: SelectionKey, channel: DatagramChannel, attachment: Attachment): Unit = {
      attachment.peekWriter match {
        case Some((p, cb)) =>
          try {
            val sent = channel.send(ByteBuffer.wrap(p.bytes.toArray), p.remote)
            if (sent > 0) {
              attachment.dequeueWriter
              cb(None)
            }
          } catch {
            case e: IOException =>
              attachment.dequeueWriter
              cb(Some(e))
          }
          if (!attachment.hasWriters) {
            key.interestOps(key.interestOps & ~SelectionKey.OP_WRITE)
            ()
          }
        case None =>
          sys.error("key marked for write but no writer")
      }
    }

    private def runPendingThunks: Unit = {
      val thunksToRun = pendingThunks.synchronized { pendingThunks.dequeueAll(_ => true) }
      thunksToRun foreach { _() }
    }

    private val selectorThread = Strategy.daemonThreadFactory("fs2-udp-selector").newThread(new Runnable {
      def run = {
        val readBuffer = ByteBuffer.allocate(1 << 16)
        while (!closed && !Thread.currentThread.isInterrupted) {
          runPendingThunks
          selectorLock.lock
          selectorLock.unlock
          selector.select
          val selectedKeys = selector.selectedKeys.iterator
          while (selectedKeys.hasNext) {
            val key = selectedKeys.next
            selectedKeys.remove
            val channel = key.channel.asInstanceOf[DatagramChannel]
            val attachment = key.attachment.asInstanceOf[Attachment]
            try {
              if (key.isValid) {
                if (key.isReadable) read1(key, channel, attachment, readBuffer)
                if (key.isWritable) write1(key, channel, attachment)
              }
            } catch {
              case t: CancelledKeyException => // Ignore; key was closed
            }
          }
        }
        close
        runPendingThunks
      }
    })
    selectorThread.start()

    override def toString = "AsynchronousSocketGroup"
  }
}
