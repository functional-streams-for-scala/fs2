package fs2.io.file

import fs2._
import fs2.Stream.Handle
import fs2.util.Monad
import java.nio.channels._
import java.nio.file._

object pulls {
  //
  // Pull constructors
  //

  /**
    * Given a `FileHandle[F]`, creates a `Pull` which reads all data from the associated file.
    */
  def readAllFromFileHandle[F[_]](chunkSize: Int)(h: FileHandle[F]): Pull[F, Byte, Unit] =
    _readAllFromFileHandle0(chunkSize, 0)(h)

  private def _readAllFromFileHandle0[F[_]](chunkSize: Int, offset: Long)(h: FileHandle[F]): Pull[F, Byte, Unit] = for {
    res <- Pull.eval(h.read(chunkSize, offset))
    next <- res.fold[Pull[F, Byte, Unit]](Pull.done)(o => Pull.output(o) >> _readAllFromFileHandle0(chunkSize, offset + o.size)(h))
  } yield next


  /**
   * Given a `Handle[F, Byte]` and `FileHandle[F]`, writes all data from the `Handle` to the file.
   */
  def writeAllToFileHandle[F[_]](in: Handle[F, Byte], out: FileHandle[F])(implicit F: Monad[F]): Pull[F, Nothing, Unit] =
    _writeAllToFileHandle1(in, out, 0)

  private def _writeAllToFileHandle1[F[_]](in: Handle[F, Byte], out: FileHandle[F], offset: Long)(implicit F: Monad[F]): Pull[F, Nothing, Unit] = for {
    hd #: tail <- in.await
    _ <- _writeAllToFileHandle2(hd, out, offset)
    next <- _writeAllToFileHandle1(tail, out, offset + hd.size)
  } yield next

  private def _writeAllToFileHandle2[F[_]](buf: Chunk[Byte], out: FileHandle[F], offset: Long)(implicit F: Monad[F]): Pull[F, Nothing, Unit] =
    Pull.eval(out.write(buf, offset)) flatMap { (written: Int) =>
      if (written >= buf.size)
        Pull.pure(())
      else
        _writeAllToFileHandle2(buf.drop(written), out, offset + written)
    }

  /**
    * Creates a `Pull` which allows synchronous file operations against the file at the specified `java.nio.file.Path`.
    *
    * The `Pull` closes the acquired `java.nio.channels.FileChannel` when it is done.
    */
  def fromPath[F[_]](path: Path, flags: Seq[OpenOption])(implicit F: Monad[F]): Pull[F, Nothing, FileHandle[F]] =
    fromFileChannel(F.suspend(FileChannel.open(path, flags: _*)))

  /**
    * Creates a `Pull` which allows asynchronous file operations against the file at the specified `java.nio.file.Path`.
    *
    * The `Pull` closes the acquired `java.nio.channels.AsynchronousFileChannel` when it is done.
    */
  def fromPathAsync[F[_]](path: Path, flags: Seq[OpenOption])(implicit F: Async[F], S: Strategy): Pull[F, Nothing, FileHandle[F]] =
    fromAsynchronousFileChannel(F.suspend(AsynchronousFileChannel.open(path, flags: _*)))

  /**
    * Given a `java.nio.channels.FileChannel`, will create a `Pull` which allows synchronous operations against the underlying file.
    *
    * The `Pull` closes the provided `java.nio.channels.FileChannel` when it is done.
    */
  def fromFileChannel[F[_]](channel: F[FileChannel])(implicit F: Monad[F]): Pull[F, Nothing, FileHandle[F]] =
    Pull.acquire(F.map(channel)(FileHandle.fromFileChannel[F]))(_.close())

  /**
    * Given a `java.nio.channels.AsynchronousFileChannel`, will create a `Pull` which allows asynchronous operations against the underlying file.
    *
    * The `Pull` closes the provided `java.nio.channels.AsynchronousFileChannel` when it is done.
    */
  def fromAsynchronousFileChannel[F[_]](channel: F[AsynchronousFileChannel])(implicit F: Async[F], S: Strategy): Pull[F, Nothing, FileHandle[F]] =
    Pull.acquire(F.map(channel)(FileHandle.fromAsynchronousFileChannel[F]))(_.close())
}
