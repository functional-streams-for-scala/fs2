package scalaz.stream

import java.io.{ InputStream, OutputStream }
import java.lang.{ Process => SysProcess, ProcessBuilder }
import scala.io.{ Codec, Source }
import scalaz.concurrent.Task
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.semigroup._
import Process._

/*
TODO:
- Naming: Having Process and Subprocess as types that are unrelated is
  unfortunate. Possible alternative are: Sys(tem)Exchange, ProcExchange,
  SystemProc, ChildProc, ProgramExchange?

- Expose the return value of Process.waitFor().

- Support ProcessBuilder's directory, environment, redirectErrrorStream methods.
  Possibly by adding more parameters the the create?Process methods.

- Support terminating a running Subprocess via Process.destory().

- Find better names for createRawProcess and createLineProcess.
*/

case class Subprocess[+R, -W](
  input: Sink[Task, W],
  output: Process[Task, R],
  error: Process[Task, R]) {

  def contramapW[W2](f: W2 => W): Subprocess[R, W2] =
    Subprocess(input.contramap(f), output, error)

  def mapR[R2](f: R => R2): Subprocess[R2, W] =
    Subprocess(input, output.map(f), error.map(f))

  def mapSink[W2](f: Sink[Task, W] => Sink[Task, W2]): Subprocess[R, W2] =
    Subprocess(f(input), output, error)

  def mapSources[R2](f: Process[Task, R] => Process[Task, R2]): Subprocess[R2, W] =
    Subprocess(input, f(output), f(error))
}

object Subprocess {
  def createRawProcess(args: String*): Process[Task, Subprocess[Bytes, Bytes]] =
    io.resource(
      Task.delay(new ProcessBuilder(args: _*).start))(
      p => Task.delay(close(p)))(
      p => Task.delay(mkRawSubprocess(p))).once

  def createLineProcess(args: String*)(implicit codec: Codec): Process[Task, Subprocess[String, String]] =
    createRawProcess(args: _*).map {
      _.mapSources(_.pipe(lines)).contramapW(s => Bytes.unsafe(s.getBytes(codec.charSet)))
    }

  private def mkRawSubprocess(p: SysProcess): Subprocess[Bytes, Bytes] =
    Subprocess(
      mkRawSink(p.getOutputStream),
      mkRawSource(p.getInputStream),
      mkRawSource(p.getErrorStream))

  private def mkRawSink(os: OutputStream): Sink[Task, Bytes] =
    io.channel {
      (bytes: Bytes) => Task.delay {
        os.write(bytes.toArray)
        os.flush
      }
    }

  private def mkRawSource(is: InputStream): Process[Task, Bytes] = {
    val maxSize = 4096
    val buffer = Array.ofDim[Byte](maxSize)

    val readChunk = Task.delay {
      val size = math.min(is.available, maxSize)
      if (size > 0) {
        is.read(buffer, 0, size)
        Bytes.of(buffer, 0, size)
      } else throw End
    }
    repeatEval(readChunk)
  }

  private def close(p: SysProcess): Int = {
    p.getOutputStream.close
    p.getInputStream.close
    p.getErrorStream.close
    p.waitFor
  }

  def lines(implicit codec: Codec): Process1[Bytes, String] = {
    def isNewline(b: Byte): Boolean = {
      val c = b.toChar
      c == '\r' || c == '\n'
    }

    var carry: Option[String] = None
    process1.id[Bytes].flatMap { bytes =>
      val complete = bytes.lastOption.fold(true)(isNewline)
      val lines = Source.fromBytes(bytes.toArray).getLines.toVector

      val head = carry mappend lines.headOption
      val tail = lines.drop(1)

      val (completeLines, nextCarry) =
        if (complete)
          (head.toVector ++ tail, None)
        else if (tail.nonEmpty)
          (head.toVector ++ tail.init, tail.lastOption)
        else
          (Vector.empty, head)

      carry = nextCarry
      emitSeq(completeLines)
    }.onComplete(emitSeq(carry.toSeq))
  }
}
