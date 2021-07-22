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
package file

import cats.effect.kernel.Async
import cats.effect.kernel.Resource
import cats.syntax.all._
import fs2.internal.jsdeps.node.fsMod
import fs2.internal.jsdeps.node.fsPromisesMod
import fs2.internal.jsdeps.node.pathMod
import fs2.internal.jsdeps.node.streamMod
import fs2.io.file.ReadFiles.TemporalReadFiles
import fs2.io.file.ReadFiles.UnsealedReadFiles

import java.time.Instant
import scala.concurrent.duration._

/** Enables interacting with the file system in a way modeled on Node.js `fs` module which in turn is modeled on standard POSIX functions.
  * @see [[https://nodejs.org/api/fs.html]]
  */
sealed trait PosixFiles[F[_]] extends UnsealedReadFiles[F] {

  import PosixFiles._

  def access(path: Path, mode: AccessMode = AccessMode.F_OK): F[Boolean]

  def chmod(path: Path, mode: FileAccessMode): F[Unit]

  def chown(path: Path, uid: Long, guid: Long): F[Unit]

  def copyFile(src: Path, dest: Path, mode: CopyMode = CopyMode.Default): F[Unit]

  def lchown(path: Path, uid: Long, guid: Long): F[Unit]

  def link(existingPath: Path, newPath: Path): F[Unit]

  def lstat(path: Path): F[Stats]

  def mkdir(
      path: Path,
      recursive: Boolean = false,
      mode: FileAccessMode = FileAccessMode.Default
  ): F[Path]

  def mkdtemp(path: Path = Path.tmpdir, prefix: String = ""): Resource[F, Path]

  def open(
      path: Path,
      flags: Flags = Flags.r,
      mode: FileAccessMode = FileAccessMode.OpenDefault
  ): Resource[F, FileHandle[F]]

  def opendir(path: Path): Stream[F, Path]

  def readCursor(path: Path, flags: Flags = Flags.r): Resource[F, ReadCursor[F]]

  def readAll(path: Path, flags: Flags = Flags.r): Stream[F, Byte]

  def readlink(path: Path): F[Path]

  def realpath(path: Path): F[Path]

  def rename(oldPath: Path, newPath: Path): F[Unit]

  def rm(
      path: Path,
      force: Boolean = false,
      maxRetries: Int = 0,
      recursive: Boolean = false,
      retryDelay: FiniteDuration = 100.milliseconds
  ): F[Unit]

  def rmdir(path: Path, maxRetries: Int = 0, retryDelay: FiniteDuration = 100.milliseconds): F[Unit]

  def stat(path: Path): F[Stats]

  def unlink(path: Path): F[Unit]

  def walk(path: Path, maxDepth: Int = Int.MaxValue): Stream[F, Path]

  def writeAll(
      path: Path,
      flags: Flags = Flags.w,
      mode: FileAccessMode = FileAccessMode.OpenDefault
  ): Pipe[F, Byte, INothing]

  def writeCursor(
      path: Path,
      flags: Flags = Flags.w,
      mode: FileAccessMode = FileAccessMode.OpenDefault
  ): Resource[F, WriteCursor[F]]

  def writeCursorFromFileHandle(
      file: FileHandle[F],
      append: Boolean
  ): F[WriteCursor[F]]

  def writeRotate(
      computePath: F[Path],
      limit: Long,
      flags: Flags = Flags.w,
      mode: FileAccessMode = FileAccessMode.OpenDefault
  ): Pipe[F, Byte, INothing]
}

object PosixFiles {
  def apply[F[_]](implicit F: PosixFiles[F]): F.type = F

  final class AccessMode private (private[file] val mode: Long) extends AnyVal {
    def |(that: AccessMode) = AccessMode(this.mode | that.mode)
    def >=(that: AccessMode) = (this.mode & ~that.mode) == 0
  }
  object AccessMode {
    private def apply(mode: Long) = new AccessMode(mode)
    val F_OK = AccessMode(fsMod.constants.F_OK.toLong)
    val R_OK = AccessMode(fsMod.constants.R_OK.toLong)
    val W_OK = AccessMode(fsMod.constants.W_OK.toLong)
    val X_OK = AccessMode(fsMod.constants.X_OK.toLong)
  }

  final class CopyMode private (private[file] val mode: Long) extends AnyVal {
    def |(that: CopyMode) = CopyMode(this.mode | that.mode)
    def >=(that: CopyMode) = (this.mode & ~that.mode) == 0
  }
  object CopyMode {
    def apply(mode: Long) = new CopyMode(mode)
    val COPYFILE_EXCL = CopyMode(fsMod.constants.COPYFILE_EXCL.toLong)
    val COPYFILE_FICLONE = CopyMode(fsMod.constants.COPYFILE_FICLONE.toLong)
    val COPYFILE_FICLONE_FORCE = CopyMode(fsMod.constants.COPYFILE_FICLONE_FORCE.toLong)

    private[file] val Default = CopyMode(0)
  }

  sealed abstract class Flags
  object Flags {
    case object a extends Flags
    case object ax extends Flags
    case object `a+` extends Flags
    case object `ax+` extends Flags
    case object `as` extends Flags
    case object `as+` extends Flags
    case object r extends Flags
    case object `r+` extends Flags
    case object `rs+` extends Flags
    case object w extends Flags
    case object wx extends Flags
    case object `w+` extends Flags
    case object `wx+` extends Flags
  }

  final class FileMode private (private val mode: Long) extends AnyVal {
    def |(that: FileMode) = FileMode(this.mode | that.mode)
    def >=(that: FileMode) = (this.mode & ~that.mode) == 0
    def `type`: FileTypeMode = FileTypeMode(FileTypeMode.S_IFMT.mode & mode)
    def access: FileAccessMode = FileAccessMode(~FileTypeMode.S_IFMT.mode & mode)
  }
  object FileMode {
    def apply(`type`: FileTypeMode, access: FileAccessMode): FileMode =
      apply(`type`.mode | access.mode)
    private[file] def apply(mode: Long) = new FileMode(mode)
  }

  final class FileTypeMode private (private[file] val mode: Long) extends AnyVal {
    def |(that: FileTypeMode) = FileTypeMode(this.mode | that.mode)
    def >=(that: FileTypeMode) = (this.mode & ~that.mode) == 0
  }

  object FileTypeMode {
    private[file] def apply(mode: Long) = new FileTypeMode(mode)
    val S_IFREG = FileTypeMode(fsMod.constants.S_IFREG.toLong)
    val S_IFDIR = FileTypeMode(fsMod.constants.S_IFDIR.toLong)
    val S_IFCHR = FileTypeMode(fsMod.constants.S_IFCHR.toLong)
    val S_IFBLK = FileTypeMode(fsMod.constants.S_IFBLK.toLong)
    val S_IFIFO = FileTypeMode(fsMod.constants.S_IFIFO.toLong)
    val S_IFLNK = FileTypeMode(fsMod.constants.S_IFLNK.toLong)
    val S_IFSOCK = FileTypeMode(fsMod.constants.S_IFSOCK.toLong)
    val S_IFMT = FileTypeMode(fsMod.constants.S_IFMT.toLong)
  }

  final class FileAccessMode private (private[file] val mode: Long) extends AnyVal {
    def |(that: FileAccessMode) = FileAccessMode(this.mode | that.mode)
    def >=(that: FileAccessMode) = (this.mode & ~that.mode) == 0
  }
  object FileAccessMode {
    private[file] def apply(mode: Long) = new FileAccessMode(mode)
    val S_IRUSR = FileAccessMode(1 << 8)
    val S_IWUSR = FileAccessMode(1 << 7)
    val S_IXUSR = FileAccessMode(1 << 6)
    val S_IRWXU = S_IRUSR | S_IWUSR | S_IXUSR
    val S_IRGRP = FileAccessMode(1 << 5)
    val S_IWGRP = FileAccessMode(1 << 4)
    val S_IXGRP = FileAccessMode(1 << 3)
    val S_IRWXG = S_IRGRP | S_IWGRP | S_IXGRP
    val S_IROTH = FileAccessMode(1 << 2)
    val S_IWOTH = FileAccessMode(1 << 1)
    val S_IXOTH = FileAccessMode(1 << 0)
    val S_IRWXO = S_IROTH | S_IWOTH | S_IXOTH

    private[file] val Default = S_IRWXU | S_IRWXG | S_IRWXO
    private[file] val OpenDefault = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH
  }

  sealed trait Stats {
    def isBlockDevice: Boolean
    def isCharacterDevice: Boolean
    def isDirectory: Boolean
    def isFIFO: Boolean
    def isFile: Boolean
    def isSocket: Boolean
    def isSymbolicLink: Boolean
    def dev: Long
    def ino: Long
    def nlink: Long
    def uid: Long
    def gid: Long
    def rdev: Long
    def mode: FileMode
    def size: Long
    def blksize: Long
    def blocks: Long
    def atime: Instant
    def mtime: Instant
    def ctime: Instant
    def birthtime: Instant
  }

  implicit def forAsync[F[_]: Async]: PosixFiles[F] = new AsyncPosixFiles[F]

  private final class AsyncPosixFiles[F[_]](implicit F: Async[F])
      extends TemporalReadFiles[F]
      with PosixFiles[F] {

    override def access(path: Path, mode: AccessMode): F[Boolean] =
      F.fromPromise(F.delay(fsPromisesMod.access(path.toJS, mode.mode.toDouble)))
        .redeem(_ => false, _ => true)

    override def chmod(path: Path, mode: FileAccessMode): F[Unit] =
      F.fromPromise(F.delay(fsPromisesMod.chmod(path.toJS, mode.mode.toDouble)))

    override def chown(path: Path, uid: Long, guid: Long): F[Unit] =
      F.fromPromise(F.delay(fsPromisesMod.chown(path.toJS, uid.toDouble, guid.toDouble)))

    override def copyFile(src: Path, dest: Path, mode: CopyMode): F[Unit] =
      F.fromPromise(F.delay(fsPromisesMod.copyFile(src.toJS, dest.toJS, mode.mode.toDouble)))

    override def lchown(path: Path, uid: Long, guid: Long): F[Unit] =
      F.fromPromise(F.delay(fsPromisesMod.lchown(path.toJS, uid.toDouble, guid.toDouble)))

    override def link(existingPath: Path, newPath: Path): F[Unit] =
      F.fromPromise(F.delay(fsPromisesMod.link(existingPath.toJS, newPath.toJS)))

    override def lstat(path: Path): F[Stats] =
      F.fromPromise(F.delay(fsPromisesMod.lstat(path.toJS))).map(new WrappedJSStats(_))

    override def mkdir(path: Path, recursive: Boolean, mode: FileAccessMode): F[Path] =
      F.fromPromise(
        F.delay(
          fsPromisesMod.mkdir(
            path.toJS,
            fsMod.MakeDirectoryOptions().setRecursive(recursive).setMode(mode.mode.toDouble)
          )
        )
      ).map(_.fold(path)(Path(_)))

    override def mkdtemp(path: Path, prefix: String): Resource[F, Path] =
      Resource
        .make(
          F.fromPromise(F.delay(fsPromisesMod.mkdtemp(path.toString + pathMod.sep + prefix)))
            .map(Path(_))
        )(
          rm(_, force = true, recursive = true)
        )

    override def open(
        path: Path,
        flags: Flags,
        mode: FileAccessMode
    ): Resource[F, FileHandle[F]] =
      Resource
        .make(
          F.fromPromise(
            F.delay(fsPromisesMod.open(path.toJS, flags.toString, mode.mode.toDouble))
          )
        )(fd => F.fromPromise(F.delay(fd.close())))
        .map(FileHandle.make[F])

    override def opendir(path: Path): Stream[F, Path] =
      Stream
        .bracket(F.fromPromise(F.delay(fsPromisesMod.opendir(path.toString()))))(dir =>
          F.fromPromise(F.delay(dir.close()))
        )
        .flatMap { dir =>
          Stream
            .repeatEval(F.fromPromise(F.delay(dir.read())))
            .map(dirent => Option(dirent.asInstanceOf[fsMod.Dirent]))
            .unNoneTerminate
            .map(dirent => path / Path(dirent.name))
        }

    override def readCursor(path: Path): Resource[F, ReadCursor[F]] =
      readCursor(path, Flags.r)

    override def readCursor(path: Path, flags: Flags): Resource[F, ReadCursor[F]] =
      open(path, flags).map(ReadCursor(_, 0L))

    override def readAll(path: Path, chunkSize: Int): Stream[F, Byte] =
      readAll(path, Flags.r)

    override def readAll(path: Path, flags: Flags): Stream[F, Byte] =
      readReadable(
        F.delay(
          fsMod
            .createReadStream(
              path.toJS,
              fsMod.ReadStreamOptions().setFlags(flags.toString)
            )
            .asInstanceOf[Readable]
        )
      )

    override def readRange(path: Path, chunkSize: Int, start: Long, end: Long): Stream[F, Byte] =
      readReadable(
        F.delay(
          fsMod
            .createReadStream(
              path.toJS,
              fsMod
                .ReadStreamOptions()
                .setFlags(Flags.r.toString)
                .setStart(start.toDouble)
                .setEnd((end - 1).toDouble)
            )
            .asInstanceOf[Readable]
        )
      )

    override def readlink(path: Path): F[Path] =
      F.fromPromise(F.delay(fsPromisesMod.readlink(path.toJS))).map(Path(_))

    override def realpath(path: Path): F[Path] =
      F.fromPromise(F.delay(fsPromisesMod.realpath(path.toJS))).map(Path(_))

    override def rename(oldPath: Path, newPath: Path): F[Unit] =
      F.fromPromise(F.delay(fsPromisesMod.rename(oldPath.toJS, newPath.toJS)))

    override def rm(
        path: Path,
        force: Boolean,
        maxRetries: Int,
        recursive: Boolean,
        retryDelay: FiniteDuration
    ): F[Unit] =
      F.fromPromise(
        F.delay(
          fsPromisesMod.rm(
            path.toJS,
            fsMod
              .RmOptions()
              .setForce(force)
              .setMaxRetries(maxRetries.toDouble)
              .setRecursive(recursive)
              .setRetryDelay(retryDelay.toMillis.toDouble)
          )
        )
      )

    override def rmdir(path: Path, maxRetries: Int, retryDelay: FiniteDuration): F[Unit] =
      F.fromPromise(
        F.delay(
          fsPromisesMod.rmdir(
            path.toJS,
            fsMod
              .RmDirOptions()
              .setMaxRetries(maxRetries.toDouble)
              .setRetryDelay(retryDelay.toMillis.toDouble)
          )
        )
      )

    override def stat(path: Path): F[Stats] =
      F.fromPromise(F.delay(fsPromisesMod.stat(path.toJS))).map(new WrappedJSStats(_))

    override def unlink(path: Path): F[Unit] =
      F.fromPromise(F.delay(fsPromisesMod.unlink(path.toJS)))

    override def walk(path: Path, maxDepth: Int): Stream[F, Path] =
      Stream.emit(path) ++ (if (maxDepth > 0)
                              Stream.eval(stat(path)).filter(_.isDirectory) >> opendir(path)
                                .flatMap(walk(_, maxDepth - 1))
                            else Stream.empty)

    override def writeCursor(
        path: Path,
        flags: Flags,
        mode: FileAccessMode
    ): Resource[F, WriteCursor[F]] =
      open(path, flags, mode).map(WriteCursor(_, 0L))

    override def writeAll(path: Path, flags: Flags, mode: FileAccessMode): Pipe[F, Byte, INothing] =
      writeWritable(
        F.delay(
          fsMod
            .createWriteStream(
              path.toJS,
              fsMod.StreamOptions().setFlags(flags.toString).setMode(mode.mode.toDouble)
            )
            .asInstanceOf[Writable]
        )
      )

    override def writeCursorFromFileHandle(
        file: FileHandle[F],
        append: Boolean
    ): F[WriteCursor[F]] =
      if (append) file.size.map(s => WriteCursor(file, s)) else WriteCursor(file, 0L).pure[F]

    override def writeRotate(
        computePath: F[Path],
        limit: Long,
        flags: Flags,
        mode: FileAccessMode
    ): Pipe[F, Byte, INothing] = {
      def openNewFile: Resource[F, FileHandle[F]] =
        Resource
          .eval(computePath)
          .flatMap(p => open(p, flags, mode))

      def newCursor(file: FileHandle[F]): F[WriteCursor[F]] =
        writeCursorFromFileHandle(file, flags.toString.contains('a'))

      internal.WriteRotate(openNewFile, newCursor, limit)
    }
  }

  private final class WrappedJSStats(private val stats: fsMod.Stats) extends Stats {
    override def isBlockDevice: Boolean = stats.isBlockDevice()
    override def isCharacterDevice: Boolean = stats.isCharacterDevice()
    override def isDirectory: Boolean = stats.isDirectory()
    override def isFIFO: Boolean = stats.isFIFO()
    override def isFile: Boolean = stats.isFile()
    override def isSocket: Boolean = stats.isSocket()
    override def isSymbolicLink: Boolean = stats.isSymbolicLink()
    override def dev: Long = stats.dev.toLong
    override def ino: Long = stats.ino.toLong
    override def nlink: Long = stats.nlink.toLong
    override def uid: Long = stats.uid.toLong
    override def gid: Long = stats.gid.toLong
    override def rdev: Long = stats.rdev.toLong
    override def mode: FileMode = FileMode(stats.mode.toLong)
    override def size: Long = stats.size.toLong
    override def blksize: Long = stats.blksize.toLong
    override def blocks: Long = stats.blocks.toLong
    override def atime: Instant = Instant.ofEpochMilli(stats.atimeMs.toLong)
    override def mtime: Instant = Instant.ofEpochMilli(stats.mtimeMs.toLong)
    override def ctime: Instant = Instant.ofEpochMilli(stats.ctimeMs.toLong)
    override def birthtime: Instant = Instant.ofEpochMilli(stats.birthtimeMs.toLong)
  }

}
