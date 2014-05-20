package scalaz.stream

import scalaz._
import scalaz.syntax.equal._
import scalaz.std.anyVal._
import scalaz.std.list._

import org.scalacheck._
import Prop._
import scalaz.concurrent.Strategy
import scala.concurrent
import scalaz.\/._

import TestInstances._

object ProcessSpec extends Properties("Process") {

  import Process._
  import process1._

  implicit val S = Strategy.DefaultStrategy

  // Subtyping of various Process types:
  // * Process1 is a Tee that only read from the left (Process1[I,O] <: Tee[I,Any,O])
  // * Tee is a Wye that never requests Both (Tee[I,I2,O] <: Wye[I,I2,O])
  // This 'test' is just ensuring that this typechecks
  object Subtyping {
    def asTee[I,O](p1: Process1[I,O]): Tee[I,Any,O] = p1
    def asWye[I,I2,O](t: Tee[I,I2,O]): Wye[I,I2,O] = t
  }

  property("basic") = forAll { (p: Process0[Int], p2: Process0[String]) =>
    val f = (x: Int) => List.range(1, x.min(100))

    ("map" |: {
      (p.toList.map(_ + 1) === p.map(_ + 1).toList) &&
      (p.map(_ + 1) === p.pipe(lift(_ + 1)))
    }) &&
    ("flatMap" |: {
      (p.toList.flatMap(f) === p.flatMap(f andThen Process.emitAll).toList)
    }) &&
    ("zip" |: {
      (p.toList.zip(p2.toList) === p.zip(p2).toList)
    }) &&
    ("yip" |: {
      val l = p.toList.zip(p2.toList)
      val r = p.toSource.yip(p2.toSource).runLog.timed(3000).run.toList
      (l === r)
    })
  }

  property("fill") = forAll(Gen.choose(0,30) flatMap (i => Gen.choose(0,50) map ((i,_)))) {
    case (n,chunkSize) => Process.fill(n)(42, chunkSize).runLog.run.toList == List.fill(n)(42)
  }

  property("iterate") = secure {
    Process.iterate(0)(_ + 1).take(100).runLog.run.toList == List.iterate(0, 100)(_ + 1)
  }

  property("unfold") = secure {
    Process.unfold((0, 1)) {
      case (f1, f2) => if (f1 <= 13) Some(((f1, f2), (f2, f1 + f2))) else None
    }.map(_._1).runLog.run.toList == List(0, 1, 1, 2, 3, 5, 8, 13)
  }

  import scalaz.concurrent.Task

  property("enqueue") = secure {
    val tasks = Process.range(0,1000).map(i => Task { Thread.sleep(1); 1 })
    tasks.sequence(50).pipe(processes.sum[Int].last).runLog.run.head == 1000 &&
    tasks.gather(50).pipe(processes.sum[Int].last).runLog.run.head == 1000
  }

  // ensure that wye terminates
  property("wye one side infinite") = secure {
    import ReceiveY._
    def whileBoth[A,B]: Wye[A,B,Nothing] = {
      def go: Wye[A,B,Nothing] = receiveBoth[A,B,Nothing] {
        case HaltL(_) | HaltR(_) => halt
        case _ => go
      }
      go
    }
    val inf = Process.constant(0)
    val one = eval(Task.now(1))
    val empty = Process[Int]()
    inf.wye(empty)(whileBoth).run.timed(800).attempt.run == \/-(()) &&
    empty.wye(inf)(whileBoth).run.timed(800).attempt.run == \/-(()) &&
    inf.wye(one)(whileBoth).run.timed(800).attempt.run == \/-(()) &&
    one.wye(inf)(whileBoth).run.timed(800).attempt.run == \/-(())
  }

  property("wye runs cleanup for both sides") = secure {
    import ReceiveY._
    import java.util.concurrent.atomic.AtomicBoolean
    def eitherWhileBoth[A,B]: Wye[A,B,A \/ B] = {
      def go: Wye[A,B,A \/ B] = receiveBoth[A,B,A \/ B] {
        case HaltL(_) | HaltR(_) => halt
        case ReceiveL(i) => emit(-\/(i)) fby go
        case ReceiveR(i) => emit(\/-(i)) fby go
      }
      go
    }
    val completed = new AtomicBoolean(false)
    val (_, qProc) = async.queue[Unit]
    val left = qProc.onComplete(eval(Task.delay { completed.set(true) }))
    val right = Process[Int](1)
    left.wye(right)(eitherWhileBoth).run.run
    completed.get
  }

  property("wye runs cleanup from last evaluated await") = secure {
    import ReceiveY._
    import java.util.concurrent.atomic.AtomicInteger
    def whileBoth[A,B]: Wye[A,B,Nothing] = {
      def go: Wye[A,B,Nothing] = receiveBoth[A,B,Nothing] {
        case HaltL(_) | HaltR(_) => halt
        case _ => go
      }
      go
    }
    val openComplete = new concurrent.SyncVar[Unit]
    val nOpened = new AtomicInteger
    val open: Task[Unit] = Task.delay { nOpened.incrementAndGet(); openComplete.put(()) }
    val close: Task[Unit] = Task.delay { nOpened.decrementAndGet() }
    val (q, qProc) = async.queue[Unit]
    val (_, block) = async.queue[Unit]
    val resourceProc = await(open)(_ => block, halt, halt).onComplete(eval_(close))
    val complexProc = Process.suspend(resourceProc)
    Task { openComplete.get; q.close }.runAsync(_ => ())
    // Left side opens the resource and blocks, right side terminates. Resource must be closed.
    complexProc.wye(qProc)(whileBoth).run.run
    nOpened.get == 0
  }

  // ensure that zipping terminates when the smaller stream runs out
  property("zip one side infinite") = secure {
    val ones = Process.eval(Task.now(1)).repeat
    val p = Process(1,2,3)
    ones.zip(p).runLog.run == IndexedSeq(1 -> 1, 1 -> 2, 1 -> 3) &&
    p.zip(ones).runLog.run == IndexedSeq(1 -> 1, 2 -> 1, 3 -> 1)
  }

  property("merge") = secure {
    import scala.concurrent.duration._
    val sleepsL = Process.awakeEvery(1 seconds).take(3)
    val sleepsR = Process.awakeEvery(100 milliseconds).take(30)
    val sleeps = sleepsL merge sleepsR
    val p = sleeps.toTask
    val tasks = List.fill(10)(p.timed(500).attemptRun)
    tasks.forall(_.isRight)
  }

  property("forwardFill") = secure {
    import scala.concurrent.duration._
    val t2 = Process.awakeEvery(2 seconds).forwardFill.zip {
             Process.awakeEvery(100 milliseconds).take(100)
           }.run.timed(15000).run
    true
  }

  property("range") = secure {
    Process.range(0, 100).runLog.run == IndexedSeq.range(0, 100) &&
    Process.range(0, 1).runLog.run == IndexedSeq.range(0, 1) &&
    Process.range(0, 0).runLog.run == IndexedSeq.range(0, 0)
  }

  property("ranges") = forAll(Gen.choose(1, 101)) { size =>
    Process.ranges(0, 100, size).flatMap { case (i,j) => emitSeq(i until j) }.runLog.run ==
    IndexedSeq.range(0, 100)
  }

  property("liftL") = secure {
    import scalaz.\/._
    val s = Process.range(0, 100000)
    val p = s.map(left) pipe process1.id[Int].liftL
    true
  }

  property("feedL") = secure {
    val w = wye.feedL(List.fill(10)(1))(process1.id)
    val x = Process.range(0,100).wye(halt)(w).runLog.run
    x.toList == (List.fill(10)(1) ++ List.range(0,100))
  }

  property("feedR") = secure {
    val w = wye.feedR(List.fill(10)(1))(wye.merge[Int])
    val x = Process.range(0,100).wye(halt)(w).runLog.run
    x.toList == (List.fill(10)(1) ++ List.range(0,100))
  }

  property("either") = secure {
    val w = wye.either[Int,Int]
    val s = Process.constant(1).take(1)
    s.wye(s)(w).runLog.run.map(_.fold(identity, identity)).toList == List(1,1)
  }

  property("state") = secure {
    val s = Process.state((0, 1))
    val fib = Process(0, 1) ++ s.flatMap { case (get, set) =>
      val (prev0, prev1) = get
      val next = prev0 + prev1
      eval(set((prev1, next))).drain ++ emit(next)
    }
    val l = fib.take(10).runLog.run.toList
    l === List(0, 1, 1, 2, 3, 5, 8, 13, 21, 34)
  }

  property("duration") =  {
    val firstValueDiscrepancy = duration.take(1).runLast.run.get
    val reasonableError = 200 * 1000000 // 200 millis
    (firstValueDiscrepancy.toNanos < reasonableError) :| "duration is near zero at first access"
  }

  property("zipAll") = forAll((l: IndexedSeq[Int], l2: IndexedSeq[Int]) => {
    val a = Process.range(0,l.length).map(l(_))
    val b = Process.range(0,l2.length).map(l2(_))
    val r = a.tee(b)(tee.zipAll(-1, 1)).runLog.run.toList
    r.toString |: (r == l.zipAll(l2, -1, 1).toList)
  })

  property("passL/R") = secure {
    val a = Process.range(0,10)
    val b: Process[Task,Int] = halt
    a.tee(b)(tee.passL[Int]).runLog.run == List.range(0,10) &&
    b.tee(a)(tee.passR[Int]).runLog.run == List.range(0,10)
  }

  property("cleanup") = secure {
    val a = Process(false).toSource |> await1[Boolean]
    val b = a.orElse(Process.emit(false), Process.emit(true))
    b.cleanup.runLastOr(false).run
  }

  property("onFailure") = secure {
    @volatile var i: Int = 0
    val p = eval(Task.delay(sys.error("FAIL"))) onFailure (Process.emit(1)) map (j => i = j)
    try { p.run.run; false }
    catch { case e: Throwable =>
      e.getMessage == "FAIL" && i == 1
    }
  }

  property("interrupt") = secure {
    val p1 = Process(1,2,3,4,6).toSource
    val i1 = repeatEval(Task.now(false))
    val v = i1.wye(p1)(wye.interrupt).runLog.run.toList
    v == List(1,2,3,4,6)
  }
   import scala.concurrent.duration._
  val smallDelay = Gen.choose(10, 300) map {_.millis}


  property("every") =
    forAll(smallDelay) { delay: Duration =>
      type BD = (Boolean, Duration)
      val durationSinceLastTrue: Process1[BD, BD] = {
        def go(lastTrue: Duration): Process1[BD,BD] = {
          await1 flatMap { pair:(Boolean, Duration) => pair match {
            case (true , d) => emit((true , d - lastTrue)) fby go(d)
            case (false, d) => emit((false, d - lastTrue)) fby go(lastTrue)
          } }
        }
        go(0.seconds)
      }

      val draws = (600.millis / delay) min 10 // don't take forever

      val durationsSinceSpike = every(delay).
                   tee(duration)(tee zipWith {(a,b) => (a,b)}).
                   take(draws.toInt) |>
                   durationSinceLastTrue

      val result = durationsSinceSpike.runLog.run.toList
      val (head :: tail) = result

      head._1 :| "every always emits true first" &&
      tail.filter   (_._1).map(_._2).forall { _ >= delay } :| "true means the delay has passed" &&
      tail.filterNot(_._1).map(_._2).forall { _ <= delay } :| "false means the delay has not passed"
  }

  property("pipeIn") = secure {
    val q = async.boundedQueue[String]()

    val sink = q.enqueue.pipeIn(process1.lift[Int,String](_.toString))

    (Process.range(0,10) to sink).run.run
    val res = q.dequeue.take(10).runLog.run.toList
    q.close.run

    res === (0 until 10).map(_.toString).toList
  }

  // Single instance of original sink is used for all elements.
  property("pipeIn uses original sink once") = secure {
    // Sink starts by wiping `written`.
    var written = List[Int]()
    def acquire: Task[Unit] = Task.delay { written = Nil }
    def release(res: Unit): Task[Unit] = Task.now(())
    def step(res: Unit): Task[Int => Task[Unit]] = Task.now((i: Int) => Task.delay { written = written :+ i  })
    val sink = io.resource[Unit, Int => Task[Unit]](acquire)(release)(step)

    val source = Process(1, 2, 3).toSource

    val transformer: Process1[Int, Int] = processes.lift(i => i + 1)
    source.to(sink.pipeIn(transformer)).run.run

    written == List(2, 3, 4)
  }

  property("runStep") = secure {
    def go(p:Process[Task,Int], acc:Seq[Throwable \/ Int]) : Throwable \/ Seq[Throwable \/ Int] = {
      p.runStep.run match {
        case Step(-\/(e),Halt(_),Halt(_)) => \/-(acc)
        case Step(-\/(e),Halt(_), c) => go(c,acc :+ -\/(e))
        case Step(-\/(e),t,_) => go(t,acc :+ -\/(e))
        case Step(\/-(a),t,_) => go(t,acc ++ a.map(\/-(_)))
      }
    }

    val ex = new java.lang.Exception("pure")

    val p1 = Process.range(10,12)
    val p2 = Process.range(20,22) ++ Process.suspend(eval(Task.fail(ex))) onFailure(Process(100).toSource)
    val p3 = Process.await(Task.delay(1))(i=> throw ex,halt,emit(200)) //throws exception in `pure` code

    go((p1 ++ p2) onComplete p3, Vector()) match {
      case -\/(e) => false
      case \/-(c) =>
        c == List(
          right(10),right(11)
          , right(20),right(21),left(ex),right(100)
          , left(ex), right(200)
        )
    }

  }


  property("runStep.stackSafety") = secure {
    def go(p:Process[Task,Int], acc:Int) : Int = {
      p.runStep.run match {
        case Step(-\/(e),Halt(_),_) => acc
        case Step(-\/(e),t,_) => go(t,acc)
        case Step(\/-(a),t,_) => go(t,acc + a.sum)
      }
    }
    val s = 1 until 10000
    val p1 = s.foldLeft[Process[Task,Int]](halt)({case (p,n)=>Emit(Vector(n),p)})
    go(p1,0) == s.sum
  }


  property("affine") = secure {
    var cnt = 0
    (affine(eval_(Task.delay{ cnt = cnt + 1})) fby
      eval(Task.delay(cnt))).repeat.take(100)
    .run.run
    cnt == 1
  }

}
