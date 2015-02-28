package neutral.stream.async.mutable

import java.util.concurrent.atomic.AtomicInteger

import neutral.stream._
import neutral.stream.Cause._
import scalaz.concurrent.{Actor, Strategy, Task}
import neutral.stream.Process.Halt
import neutral.stream.async.immutable
import neutral.stream.{Cause, Util, Process, Sink}
import Util._

import scalaz.concurrent.Task


/**
 * Asynchronous queue interface. Operations are all nonblocking in their
 * implementations, but may be 'semantically' blocking. For instance,
 * a queue may have a bound on its size, in which case enqueuing may
 * block until there is an offsetting dequeue.
 */
trait Queue[A] {

  /**
   * A `Sink` for enqueueing values to this `Queue`.
   */
  def enqueue: Sink[Task, A]

  /**
   * Enqueue one element in this `Queue`. Resulting task will
   * terminate with failure if queue is closed or failed.
   * Please note this will get completed _after_ `a` has been successfully enqueued.
   * @param a `A` to enqueue
   */
  def enqueueOne(a: A): Task[Unit]

  /**
   * Enqueue multiple `A` values in this queue. This has same semantics as sequencing
   * repeated calls to `enqueueOne`.
   */
  def enqueueAll(xa: Seq[A]): Task[Unit]

  /**
   * Provides a process that dequeue from this queue.
   * When multiple consumers dequeue from this queue,
   * they dequeue in first-come, first-serve order.
   *
   * Please use `Topic` instead of `Queue` when all subscribers
   * need to see each value enqueued.
   */
  def dequeue: Process[Task, A]

  /**
   * The time-varying size of this `Queue`. This signal refreshes
   * only when size changes. Offsetting enqueues and dequeues may
   * not result in refreshes.
   */
  def size: neutral.stream.async.immutable.Signal[Int]

  /**
   * Closes this queue. This halts the `enqueue` `Sink` and
   * `dequeue` `Process` after any already-queued elements are
   * drained.
   *
   * After this any enqueue will fail with `Terminated(End)`,
   * and the enqueue `Sink` will terminate with `End`.
   */
  def close: Task[Unit] = failWithCause(End)


  /**
   * Kills the queue. Unlike `close`, this kills all dequeuers immediately.
   * Any subsequent enqueues will fail with `Terminated(Kill)`.
   * The returned `Task` will completed once all dequeuers and enqueuers
   * have been signalled.
   */
  def kill: Task[Unit] = failWithCause(Kill)


  /**
   * Like `kill`, except it terminates with supplied reason.
   */
  def fail(rsn: Throwable): Task[Unit] = failWithCause(Error(rsn))

  private[stream] def failWithCause(c:Cause): Task[Unit]
}


private[stream] object Queue {

  /**
   * Builds a queue, potentially with `source` producing the streams that
   * will enqueue into queue. Up to `bound` size of `A` may enqueue into queue,
   * and then all enqueue processes will wait until dequeue.
   *
   * @param bound   Size of the bound. When <= 0 the queue is `unbounded`.
   * @tparam A
   * @return
   */
  def apply[A](bound: Int = 0)(implicit S: Strategy): Queue[A] = {

    sealed trait M
    case class Enqueue (a: Seq[A], cb: Throwable \/ Unit => Unit) extends M
    case class Dequeue (ref:ConsumerRef, cb: Throwable \/ A => Unit) extends M
    case class Fail(cause: Cause, cb: Throwable \/ Unit => Unit) extends M
    case class GetSize(cb: (Throwable \/ Seq[Int]) => Unit) extends M
    case class ConsumerDone(ref:ConsumerRef) extends M

    // reference to identify differed subscribers
    class ConsumerRef


    //actually queued `A` are stored here
    var queued = Vector.empty[A]

    // when this queue fails or is closed the reason is stored here
    var closed: Option[Cause] = None

    // consumers waiting for `A`
    var consumers: Vector[(ConsumerRef, Throwable \/ A => Unit)] = Vector.empty

    // publishers waiting to be acked to produce next `A`
    var unAcked: Vector[Throwable \/ Unit => Unit] = Vector.empty

    // if at least one GetSize was received will start to accumulate sizes change.
    // when defined on left, contains sizes that has to be published to sizes topic
    // when defined on right, awaiting next change in queue to signal size change
    // when undefined, signals no subscriber for sizes yet.
    var sizes:  Option[Vector[Int] \/ ((Throwable \/ Seq[Int]) => Unit)] = None

    // signals to any callback that this queue is closed with reason
    def signalClosed[B](cb: Throwable \/ B => Unit) =
      closed.foreach(rsn => S(cb(-\/(Terminated(rsn)))))

    // signals that size has been changed.
    // either keep the last size or fill the callback
    // only updates if sizes != None
    def signalSize(sz: Int): Unit = {
      sizes = sizes.map( cur =>
        left(cur.fold (
            szs => { szs :+ sz }
            , cb => { S(cb(\/-(Seq(sz)))) ; Vector.empty[Int] }
          ))
      )
    }




    // publishes single size change
    def publishSize(cb: (Throwable \/ Seq[Int]) => Unit): Unit = {
      sizes =
        sizes match {
          case Some(sz) => sz match {
            case -\/(v) if v.nonEmpty => S(cb(\/-(v))); Some(-\/(Vector.empty[Int]))
            case _                    => Some(\/-(cb))
          }
          case None => S(cb(\/-(Seq(queued.size)))); Some(-\/(Vector.empty[Int]))
        }
    }

    //dequeue one element from the queue
    def dequeueOne(ref: ConsumerRef, cb: (Throwable \/ A => Unit)): Unit = {
      queued.headOption match {
        case Some(a) =>
          S(cb(\/-(a)))
          queued = queued.tail
          signalSize(queued.size)
          if (unAcked.size > 0 && bound > 0 && queued.size < bound) {
            val ackCount = bound - queued.size min unAcked.size
            unAcked.take(ackCount).foreach(cb => S(cb(\/-(()))))
            unAcked = unAcked.drop(ackCount)
          }

        case None =>
          val entry : (ConsumerRef, Throwable \/ A => Unit)  = (ref -> cb)
          consumers = consumers :+ entry
      }
    }

    def enqueueOne(as: Seq[A], cb: Throwable \/ Unit => Unit) = {
      import neutral.stream.Util._
      queued = queued fast_++ as

      if (consumers.size > 0 && queued.size > 0) {
        val deqCount = consumers.size min queued.size

        consumers.take(deqCount).zip(queued.take(deqCount))
        .foreach { case ((_,cb), a) => S(cb(\/-(a))) }

        consumers = consumers.drop(deqCount)
        queued = queued.drop(deqCount)
      }

      if (bound > 0 && queued.size >= bound) unAcked = unAcked :+ cb
      else S(cb(\/-(())))

      signalSize(queued.size)
    }

    def stop(cause: Cause, cb: Throwable \/ Unit => Unit): Unit = {
      closed = Some(cause)
      if (queued.nonEmpty && cause == End) {
        unAcked.foreach(cb => S(cb(-\/(Terminated(cause)))))
      } else {
        (consumers.map(_._2) ++ unAcked).foreach(cb => S(cb(-\/(Terminated(cause)))))
        consumers = Vector.empty
        sizes.flatMap(_.toOption).foreach(cb => S(cb(-\/(Terminated(cause)))))
        sizes = None
        queued = Vector.empty
      }
      unAcked = Vector.empty
      S(cb(\/-(())))
    }


    val actor: Actor[M] = Actor({ (m: M) =>
      if (closed.isEmpty) m match {
        case Dequeue(ref, cb)     => dequeueOne(ref, cb)
        case Enqueue(as, cb) => enqueueOne(as, cb)
        case Fail(cause, cb)   => stop(cause, cb)
        case GetSize(cb)     => publishSize(cb)
        case ConsumerDone(ref) =>  consumers = consumers.filterNot(_._1 == ref)

      } else m match {
        case Dequeue(ref, cb) if queued.nonEmpty => dequeueOne(ref, cb)
        case Dequeue(ref, cb)                    => signalClosed(cb)
        case Enqueue(as, cb)                     => signalClosed(cb)
        case GetSize(cb) if queued.nonEmpty      => publishSize(cb)
        case GetSize(cb)                         => signalClosed(cb)
        case Fail(_, cb)                         => S(cb(\/-(())))
        case ConsumerDone(ref)                   =>  consumers = consumers.filterNot(_._1 == ref)
      }
    })(S)


    new Queue[A] {
      def enqueue: Sink[Task, A] = Process.constant(enqueueOne _)
      def enqueueOne(a: A): Task[Unit] = enqueueAll(Seq(a))
      def dequeue: Process[Task, A] = {
        Process.await(Task.delay(new ConsumerRef))({ ref =>
          Process.repeatEval(Task.async[A](cb => actor ! Dequeue(ref, cb)))
          .onComplete(Process.eval_(Task.delay(actor ! ConsumerDone(ref))))
        })
      }

      val size: immutable.Signal[Int] = {
        val sizeSource : Process[Task,Int] =
          Process.repeatEval(Task.async[Seq[Int]](cb => actor ! GetSize(cb)))
          .flatMap(Process.emitAll)
        Signal(sizeSource.map(Signal.Set.apply), haltOnSource =  true)(S)
      }

      def enqueueAll(xa: Seq[A]): Task[Unit] = Task.async(cb => actor ! Enqueue(xa,cb))

      private[stream] def failWithCause(c: Cause): Task[Unit] = Task.async[Unit](cb => actor ! Fail(c,cb))
    }

  }


}
