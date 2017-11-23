package fs2.internal

import scala.concurrent.ExecutionContext
import java.util.concurrent.atomic.AtomicReference

/*
 * Implementation is taken from `scalaz` library, with only minor changes. See:
 *
 * https://github.com/scalaz/scalaz/blob/series/7.2.x/concurrent/src/main/scala/scalaz/concurrent/Actor.scala
 *
 * This code is copyright Andriy Plokhotnyuk, Runar Bjarnason, and other contributors,
 * and is licensed using 3-clause BSD, see LICENSE file at:
 *
 * https://github.com/scalaz/scalaz/blob/f20a68eb5bf1cea83d51583cdaed7d523464f3f7/LICENSE.txt
 */

/**
 * Processes messages of type `A`, one at a time. Messages are submitted to
 * the actor with the method `!`. Processing is typically performed asynchronously,
 * this is controlled by the provided execution context.
 *
 * Memory consistency guarantee: when each message is processed by the `handler`, any memory that it
 * mutates is guaranteed to be visible by the `handler` when it processes the next message, even if
 * the execution context runs the invocations of `handler` on separate threads. This is achieved because
 * the `Actor` reads a volatile memory location before entering its event loop, and writes to the same
 * location before suspending.
 *
 * Implementation based on non-intrusive MPSC node-based queue, described by Dmitriy Vyukov:
 * [[http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue]]
 *
 * @param handler  The message handler
 * @param onError  Exception handler, called if the message handler throws any `Throwable`.
 * @param ec       Execution context
 * @tparam A       The type of messages accepted by this actor.
 */
private[fs2] final class Actor[A](handler: A => Unit, onError: Throwable => Unit)(implicit val ec: ExecutionContext) {
  private val head = new AtomicReference[Node[A]]

  /** Pass the message `a` to the mailbox of this actor */
  def !(a: A): Unit = {
    val n = new Node(a)
    val h = head.getAndSet(n)
    if (h ne null) h.lazySet(n)
    else schedule(n)
  }

  private def schedule(n: Node[A]): Unit = ec.execute(() => act(n))

  @annotation.tailrec
  private def act(n: Node[A], i: Int = 1024): Unit = {
    try handler(n.a) catch {
      case NonFatal(ex) => onError(ex)
    }
    val n2 = n.get
    if (n2 eq null) scheduleLastTry(n)
    else if (i == 0) schedule(n2)
    else act(n2, i - 1)
  }

  private def scheduleLastTry(n: Node[A]): Unit = ec.execute(() => lastTry(n))

  private def lastTry(n: Node[A]): Unit = if (!head.compareAndSet(n, null)) act(next(n))

  @annotation.tailrec
  private def next(n: Node[A]): Node[A] = {
    val n2 = n.get
    if (n2 ne null) n2
    else next(n)
  }
}

private class Node[A](val a: A) extends AtomicReference[Node[A]]

private[fs2] object Actor {

  def apply[A](handler: A => Unit, onError: Throwable => Unit = throw _)
              (implicit ec: ExecutionContext): Actor[A] = new Actor[A](handler, onError)(ec)
}
