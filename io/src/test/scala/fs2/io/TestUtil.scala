package fs2.io

import java.net.InetSocketAddress
import java.nio.channels.spi.AsynchronousChannelProvider
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import org.scalacheck.Prop


object TestUtil {

  case class GroupName(get: String)

  implicit def DefaultAsynchronousChannelGroup(implicit name: GroupName) = {
    val idx = new AtomicInteger(0)
    AsynchronousChannelProvider.provider().openAsynchronousChannelGroup(
      8
      , new ThreadFactory {
        def newThread(r: Runnable): Thread = {
          val t = new Thread(r, s"fs2-AG-${name.get}-${idx.incrementAndGet() }")
          t.setDaemon(true)
          t
        }
      }
    )
  }



  def localAddress(port: Int) = new InetSocketAddress("localhost", port)
  val localBindAddress = localAddress(9999)


  // the io specs may fight for local machine resources
  // to prevent race conditions between resource specs, lock assures only one spec will go at time
  val specLock = new ReentrantLock()
  def acquireLock(prop : => Prop):Prop = {
    try {  specLock.lock(); prop }
    finally { specLock.unlock() }
  }

}