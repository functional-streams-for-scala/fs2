package scalaz.stream

import Process._
import org.scalacheck.Prop._
import org.scalacheck.Properties
import scalaz.concurrent.Task
import concurrent.duration._
import scalaz.stream.wye.Request


class ExchangeSpec extends Properties("Exchange") {

  implicit val scheduler =  scalaz.stream.DefaultScheduler

  property("loopBack") = protect {
    val xs = 1 until 10
    val l = Exchange.loopBack[String, Int](process1.lift[Int, String](_.toString))
    l.flatMap(_.run(emitAll(xs))).take(xs.size).runLog.run.toSeq == xs.map(_.toString)
  }

  property("emitHead") = protect {
    val xs = 1 until 10
    val l = Exchange.loopBack[Int, Int](emitAll(xs) ++ process1.id)
    l.flatMap(_.run(emitAll(xs))).take(xs.size + xs.size / 2).runLog.run.toSeq == xs ++ xs.take(xs.size / 2)
  }

  property("loopBack.terminate.process1") = protect {
    val xs = 1 until 10
    val l = Exchange.loopBack[Int, Int](process1.take(5))
    l.flatMap(_.run(emitAll(xs))).runLog.run.toSeq == xs.take(5)
  }


  property("mapO") = protect {
    val xs = 1 until 10
    val l = Exchange.loopBack[Int, Int](process1.id).map(_.mapO(_.toString))
    l.flatMap(_.run(emitAll(xs))).take(xs.size).runLog.run == xs.map(_.toString)
  }

  property("mapW") = protect {
    val xs = 1 until 10
    val l = Exchange.loopBack[Int, Int](process1.id).map(_.mapW[String](_.toInt))
    l.flatMap(_.run(emitAll(xs.map(_.toString)))).take(xs.size).runLog.run == xs
  }


  property("pipeBoth") = protect {
    val xs = 1 until 10
    val l =
      Exchange.loopBack[Int, Int](process1.id)
      .map(_.pipeBoth(
        process1.lift[Int, String](i => (i * 10).toString)
        , process1.lift[String, Int](s => s.toInt)
      ))

    l.flatMap(_.run(emitAll(xs.map(_.toString)))).take(xs.size).runLog.run.toSeq == xs.map(_ * 10).map(_.toString)
  }


  property("through") = protect {
    val xs = 1 until 10
    val ch: Channel[Task, Int, Process[Task, (Int, Int)]] = constant((i: Int) => Task.now(emitAll(xs).toSource.map((i, _))))
    val l = Exchange.loopBack[Int, Int](process1.id).map(_.through(ch))
    l.flatMap(_.run(emitAll(xs))).take(xs.size * xs.size).runLog.run == xs.map(i => xs.map(i2 => (i, i2))).flatten
  }

  property("run.terminate.on.read") = protect {
    val ex = Exchange[Int,Int](Process.range(1,10),Process.constant(i => Task.now(())))
    ex.run(time.sleep(1 minute)).runLog.timed(3000).run == (1 until 10).toVector
  }


  property("run.terminate.on.write") = protect {
    val ex = Exchange[Int,Int](time.sleep(1 minute),Process.constant(i => Task.now(())))
    ex.run(Process.range(1,10), Request.R).runLog.timed(3000).run == Vector()
  }

  property("run.terminate.on.read.or.write") = protect {
    val exL = Exchange[Int,Int](Process.range(1,10),Process.constant(i => Task.now(())))
    val exR = Exchange[Int,Int](time.sleep(1 minute),Process.constant(i => Task.now(())))
    ("left side terminated" |: exL.run(time.sleep(1 minute), Request.Both).runLog.timed(3000).run == (1 until 10).toVector) &&
      ("right side terminated" |: exR.run(Process.range(1,10), Request.Both).runLog.timed(3000).run == Vector())
  }

}

