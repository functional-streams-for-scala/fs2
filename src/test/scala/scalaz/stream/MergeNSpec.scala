package scalaz.stream

import Cause._
import org.scalacheck.Prop._
import org.scalacheck.Properties
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream.Process._
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.SyncVar
import scalaz.\/

class MergeNSpec extends Properties("mergeN") {

  implicit val S = Strategy.DefaultStrategy
  implicit val scheduler = scalaz.stream.DefaultScheduler

  property("basic") = forAll {
    (l: List[Int]) =>

      val count = (l.size % 6) max 1

      val ps =
        emitAll(for (i <- 0 until count) yield {
          emitAll(l.filter(v => (v % count).abs == i)).toSource
        }).toSource

      val result =
        merge.mergeN(ps).runLog.timed(3000).run.toSet

      (result.toList.sorted == l.toSet.toList.sorted) :| "All elements were collected"

  }

  property("complete-with-inner-finalizer") = protect {
    merge.mergeN(emit(halt) onComplete eval_(Task now (()))).runLog timed 3000 run

    true
  }


  // tests that when downstream terminates,
  // all cleanup code is called on upstreams
  property("source-cleanup-down-done") = protect {
    val cleanupQ = async.unboundedQueue[Int]
    val cleanups = new SyncVar[Throwable \/ IndexedSeq[Int]]

    val ps =
      emitAll(for (i <- 0 until 10) yield {
        Process.constant(i+100)  onComplete eval_(cleanupQ.enqueueOne(i))
      }).toSource onComplete eval_(cleanupQ.enqueueOne(99))

    cleanupQ.dequeue.take(11).runLog.runAsync(cleanups.put)

    // this makes sure we see at least one value from sources
    // and therefore we won`t terminate downstream to early.

    merge.mergeN(ps).scan(Set[Int]())({
      case (sum, next) => sum + next
    }).takeWhile(_.size < 10).runLog.timed(3000).run



    (cleanups.get(3000).isDefined &&
      cleanups.get(0).get.isRight &&
      cleanups.get(0).get.toList.flatten.size == 11) :| s"Cleanups were called on upstreams: ${cleanups.get(0)}"
  }


  // unlike source-cleanup-down-done it focuses on situations where upstreams are in async state,
  // and thus will block until interrupted.
  property("source-cleanup-async-down-done") = protect {
    val cleanupQ = async.unboundedQueue[Int]
    val cleanups = new SyncVar[Throwable \/ IndexedSeq[Int]]
    cleanupQ.dequeue.take(11).runLog.runAsync(cleanups.put)


    //this below is due the non-thread-safety of scala object, we must memoize this here
    val delayEach10 = time.awakeEvery(10 seconds)

    def oneUp(index:Int) =
      (emit(index).toSource ++ delayEach10.map(_=>index))
      .onComplete(eval_(cleanupQ.enqueueOne(index)))

    val ps =
      (emitAll(for (i <- 0 until 10) yield oneUp(i)).toSource ++ delayEach10.drain) onComplete
        eval_(cleanupQ.enqueueOne(99))


    merge.mergeN(ps).takeWhile(_ < 9).runLog.timed(3000).run

    (cleanups.get(3000).isDefined &&
      cleanups.get(0).get.isRight &&
      cleanups.get(0).get.toList.flatten.size == 11) :| s"Cleanups were called on upstreams: ${cleanups.get(0)}"
  }


  //merges 10k of streams, each with 100 of elements
  property("merge-million") = protect {
    val count = 1000
    val eachSize = 1000

    val ps =
      emitAll(for (i <- 0 until count) yield {
        Process.range(0,eachSize)
      }).toSource

    val result = merge.mergeN(ps).fold(0)(_ + _).runLast.timed(120000).run

    (result == Some(499500000)) :| s"All items were emitted: $result"
  }

  property("merge-maxOpen") = protect {
    val count = 100
    val eachSize = 10

    val sizeSig = async.signalUnset[Int]

    def incrementOpen =
      sizeSig.compareAndSet({
        case Some(running) => Some(running + 1)
        case None => Some(1)
      })

    def decrementDone =
      sizeSig.compareAndSet({
        case Some(running) => Some(running - 1)
        case None => Some(0)
      })

    val sleep5 = time.sleep(5 millis)

    val ps =
      emitAll(for (i <- 0 until count) yield {
        eval_(incrementOpen) ++
          Process.range(0,eachSize).flatMap(i=> emit(i) ++ sleep5) onComplete
          eval_(decrementDone)
      }).toSource

    val running = new SyncVar[Throwable \/ IndexedSeq[Int]]
    Task.fork(sizeSig.discrete.runLog).runAsync(running.put)

    merge.mergeN(25)(ps).run.timed(10000).run
    sizeSig.close.run

    "mergeN and signal finished" |: running.get(3000).isDefined &&
      (s"max 25 were run in parallel ${running.get.toList.flatten}" |: running.get.toList.flatten.filter(_ > 25).isEmpty)

  }


  //tests that mergeN correctly terminates with drained process
  property("drain-halt") = protect {

    val effect = Process.repeatEval(Task.delay(())).drain
    val p = Process(1,2)

    merge.mergeN(Process(effect,p)).take(2)
    .runLog.timed(3000).run.size == 2

  }

  // tests that if one of the processes to mergeN is killed the mergeN is killed as well.
  property("drain-kill-one") = protect {
    import TestUtil._
    val effect = Process.repeatEval(Task.delay(())).drain
    val p = Process(1,2) onComplete Halt(Kill)

    val r =
      merge.mergeN(Process(effect,p))
      .expectedCause(_ == Kill)
      .runLog.timed(3000).run

    r.size == 2
  }

  // tests that mergeN does not deadlock when the producer is waiting for enqueue to complete
  // this is really testing `njoin`
  property("bounded-mergeN-halts-onFull") = protect {
    merge.mergeN(1)(emit(constant(())))
	.once
	.run.timed(3000).run
	true
  }

  property("kill mergeN") = protect {
    merge.mergeN(Process(Process.repeatEval(Task.now(1)))).kill.run.timed(3000).run
    true // Test terminates.
  }

  property("complete all children before onComplete") = protect {
    val count = new AtomicInteger(0)
    val inc = Process eval (Task delay { count.incrementAndGet() })
    val size = 10

    val p = merge.mergeN(Process emitAll (0 until size map { _ => inc })).drain onComplete (Process eval (Task delay { count.get() }))

    val result = p.runLog timed 3000 run

    (result.length == 1) :| s"result.length == ${result.length}" &&
      (result.head == size) :| s"result.head == ${result.head}"
  }

  property("avoid hang in the presence of interrupts") = protect {
    1 to 100 forall { _ =>
      val q = async.unboundedQueue[Unit]
      q.enqueueOne(()).run

      val process = (merge.mergeN(0)(Process(q.dequeue, halt)).once wye halt)(wye.mergeHaltBoth)

      process.run.timed(3000).attempt.run.isRight
    }
  }
}
