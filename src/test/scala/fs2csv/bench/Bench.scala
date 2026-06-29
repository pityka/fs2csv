package fs2csv.bench

import java.io.{BufferedWriter, File, FileWriter}

import cats.effect.{IO, IOApp}
import fs2.io.file.{Files, Path}

import fs2csv.*

case class Trade(id: Long, name: String, score: Double, active: Boolean, note: String) derives RowReader

object Bench extends IOApp.Simple {

  override def run: IO[Unit] = {
    val rows = sys.env.get("ROWS").map(_.toLong).getOrElse(5_000_000L)
    val file = new File(sys.env.getOrElse("CSV_PATH", "/tmp/fs2csv-bench.csv"))
    for {
      _ <- IO(println(s"Generating $rows rows to $file ..."))
      _ <- IO(generateCsvFile(file, rows))
      _ <- IO(println(s"File size: ${file.length / 1024 / 1024} MB"))
      _ <- IO(println("--- raw saddle Callback ---")) *> IO(benchRawSaddleCallback(file))
      _ <- IO(println("--- CsvPipes (fs2, Files[IO].readAll default chunk=64KiB) ---")) *>
             benchFilesReadAllDefault(file)
      _ <- IO(println("--- CsvPipes (fs2, Files[IO].readAll chunk=256KiB) ---")) *>
             benchFilesReadAllWithChunkSize(file, 1 << 18)
      _ <- IO(println("--- CsvPipes (fs2, Files[IO].readAll chunk=1MiB) ---")) *>
             benchFilesReadAllWithChunkSize(file, 1 << 20)
      _ <- IO(println("--- CsvPipes (fs2, Files[IO].readAll chunk=2MiB) ---")) *>
             benchFilesReadAllWithChunkSize(file, 1 << 21)
      _ <- IO(println("--- CsvPipes (fs2, Files[IO].readAll chunk=4MiB) ---")) *>
             benchFilesReadAllWithChunkSize(file, 1 << 22)
      _ <- IO(println("--- CsvPipes (fs2, fs2.io.readInputStream + FileInputStream) ---")) *>
             benchReadInputStreamUnbuffered(file)
      _ <- IO(println("--- CsvPipes (fs2, fs2.io.readInputStream + BufferedInputStream) ---")) *>
             benchReadInputStreamBuffered(file)
      _ <- IO(println("--- CsvPipes (fs2, FileChannel + Chunk.array) ---")) *>
             benchFileChannelEmittingArrayChunks(file)
      _ <- IO(println("--- CsvPipes (fs2, FileChannel + Chunk.ByteBuffer) ---")) *>
             benchFileChannelEmittingByteBufferChunks(file)
      _ <- IO(println("--- fs2-data-csv 1.14.1, Files[IO].readAll(256KiB) + decodeUsingHeaders[Trade] ---")) *>
             benchFs2DataCsv(file)
    } yield ()
  }

  private def generateCsvFile(file: File, n: Long): Unit = {
    val w = new BufferedWriter(new FileWriter(file), 1 << 20)
    try {
      w.write("id,name,score,active,note\n")
      var i = 0L
      while (i < n) {
        w.write(java.lang.Long.toString(i))
        w.write(",name_")
        w.write(java.lang.Long.toString(i))
        w.write(",")
        w.write(java.lang.Double.toString((i % 1000) * 0.5))
        w.write(",")
        w.write(if ((i & 1L) == 0L) "true" else "false")
        w.write(",hello world\n")
        i += 1
      }
    } finally w.close()
  }

  private def benchRawSaddleCallback(file: File): Unit = {
    var cellSum: Long  = 0L
    var rowCount: Long = 0L
    val cb = new org.saddle.io.csv.Callback {
      def apply(
          chars: Array[Char],
          from:  Array[Int],
          to:    Array[Int],
          len:   Int,
          eol:   Array[Int]
      ) = {
        var i = 0
        while (i < len) {
          var hash  = 0
          var p     = from(i)
          val rawTo = to(i)
          val q     = if (rawTo < 0) -rawTo else rawTo
          while (p < q) { hash = 31 * hash + chars(p); p += 1 }
          cellSum += hash
          if (rawTo < 0) rowCount += 1
          i += 1
        }
        org.saddle.io.csv.Next
      }
    }
    val started = System.nanoTime()
    val ch = new java.io.FileInputStream(file).getChannel
    try {
      val err = org.saddle.io.csv.parse(
        channel         = ch,
        callback        = cb,
        bufferSize      = 1 << 18,
        recordSeparator = "\n"
      )
      err.foreach(e => println(s"  ERROR: $e"))
    } finally ch.close()
    report(started, rowCount, file, s"sum=$cellSum")
  }

  private def benchFilesReadAllDefault(file: File): IO[Unit] = {
    val opts    = CsvParserOptions.default.withBufferSize(1 << 18)
    val started = System.nanoTime()
    Files[IO]
      .readAll(Path.fromNioPath(file.toPath))
      .through(CsvPipes.decodeUsingHeaders[IO, Trade](opts))
      .compile
      .fold((0L, 0L, 0.0)) { case ((n, ids, pxs), t) =>
        (n + 1L, ids + t.id, pxs + t.score)
      }
      .flatMap { case (n, idSum, pxSum) =>
        IO(report(started, n, file, f"idSum=$idSum pxSum=$pxSum%.1f"))
      }
  }

  private def benchFilesReadAllWithChunkSize(file: File, chunkSize: Int): IO[Unit] = {
    val opts    = CsvParserOptions.default.withBufferSize(1 << 18)
    val started = System.nanoTime()
    Files[IO]
      .readAll(Path.fromNioPath(file.toPath), chunkSize, fs2.io.file.Flags.Read)
      .through(CsvPipes.decodeUsingHeaders[IO, Trade](opts))
      .compile
      .fold((0L, 0L, 0.0)) { case ((n, ids, pxs), t) =>
        (n + 1L, ids + t.id, pxs + t.score)
      }
      .flatMap { case (n, idSum, pxSum) =>
        IO(report(started, n, file, f"idSum=$idSum pxSum=$pxSum%.1f"))
      }
  }

  private def benchReadInputStreamBuffered(file: File): IO[Unit] = {
    val opts      = CsvParserOptions.default.withBufferSize(1 << 18)
    val chunkSize = 1 << 16
    val started   = System.nanoTime()
    fs2.io
      .readInputStream(
        IO(new java.io.BufferedInputStream(new java.io.FileInputStream(file), chunkSize): java.io.InputStream),
        chunkSize,
        closeAfterUse = true
      )
      .through(CsvPipes.decodeUsingHeaders[IO, Trade](opts))
      .compile
      .fold((0L, 0L, 0.0)) { case ((n, ids, pxs), t) =>
        (n + 1L, ids + t.id, pxs + t.score)
      }
      .flatMap { case (n, idSum, pxSum) =>
        IO(report(started, n, file, f"idSum=$idSum pxSum=$pxSum%.1f"))
      }
  }

  private def benchReadInputStreamUnbuffered(file: File): IO[Unit] = {
    val opts      = CsvParserOptions.default.withBufferSize(1 << 18)
    val chunkSize = 1 << 16
    val started   = System.nanoTime()
    fs2.io
      .readInputStream(
        IO(new java.io.FileInputStream(file): java.io.InputStream),
        chunkSize,
        closeAfterUse = true
      )
      .through(CsvPipes.decodeUsingHeaders[IO, Trade](opts))
      .compile
      .fold((0L, 0L, 0.0)) { case ((n, ids, pxs), t) =>
        (n + 1L, ids + t.id, pxs + t.score)
      }
      .flatMap { case (n, idSum, pxSum) =>
        IO(report(started, n, file, f"idSum=$idSum pxSum=$pxSum%.1f"))
      }
  }

  private def benchFileChannelEmittingArrayChunks(file: File): IO[Unit] = {
    import fs2.{Chunk, Stream}
    val opts      = CsvParserOptions.default.withBufferSize(1 << 18)
    val chunkSize = 1 << 16
    val started   = System.nanoTime()
    val bytes: Stream[IO, Byte] = Stream.bracket(
      IO(new java.io.FileInputStream(file).getChannel)
    )(c => IO(c.close())).flatMap { ch =>
      Stream.repeatEval(IO {
        val buf = java.nio.ByteBuffer.allocate(chunkSize)
        val n   = ch.read(buf)
        if (n < 0) None
        else { buf.flip(); Some(Chunk.array(buf.array(), 0, n)) }
      }).unNoneTerminate.flatMap(Stream.chunk)
    }
    bytes
      .through(CsvPipes.decodeUsingHeaders[IO, Trade](opts))
      .compile
      .fold((0L, 0L, 0.0)) { case ((n, ids, pxs), t) =>
        (n + 1L, ids + t.id, pxs + t.score)
      }
      .flatMap { case (n, idSum, pxSum) =>
        IO(report(started, n, file, f"idSum=$idSum pxSum=$pxSum%.1f"))
      }
  }

  private def benchFileChannelEmittingByteBufferChunks(file: File): IO[Unit] = {
    import fs2.{Chunk, Stream}
    val opts      = CsvParserOptions.default.withBufferSize(1 << 18)
    val chunkSize = 1 << 16
    val started   = System.nanoTime()
    val bytes: Stream[IO, Byte] = Stream.bracket(
      IO(new java.io.FileInputStream(file).getChannel)
    )(c => IO(c.close())).flatMap { ch =>
      Stream.repeatEval(IO {
        val buf = java.nio.ByteBuffer.allocate(chunkSize)
        val n   = ch.read(buf)
        if (n < 0) None
        else { buf.flip(); Some(Chunk.ByteBuffer(buf)) }
      }).unNoneTerminate.flatMap(Stream.chunk)
    }
    bytes
      .through(CsvPipes.decodeUsingHeaders[IO, Trade](opts))
      .compile
      .fold((0L, 0L, 0.0)) { case ((n, ids, pxs), t) =>
        (n + 1L, ids + t.id, pxs + t.score)
      }
      .flatMap { case (n, idSum, pxSum) =>
        IO(report(started, n, file, f"idSum=$idSum pxSum=$pxSum%.1f"))
      }
  }

  private def benchFs2DataCsv(file: File): IO[Unit] = {
    import fs2.data.csv.{decodeUsingHeaders, CsvRowDecoder}
    import fs2.data.csv.generic.semiauto.deriveCsvRowDecoder
    given CsvRowDecoder[Trade, String] = deriveCsvRowDecoder[Trade]

    val started = System.nanoTime()
    Files[IO]
      .readAll(Path.fromNioPath(file.toPath), 1 << 18, fs2.io.file.Flags.Read)
      .through(fs2.text.utf8.decode)
      .through(decodeUsingHeaders[Trade]())
      .compile
      .fold((0L, 0L, 0.0)) { case ((n, ids, pxs), t) =>
        (n + 1L, ids + t.id, pxs + t.score)
      }
      .flatMap { case (n, idSum, pxSum) =>
        IO(report(started, n, file, f"idSum=$idSum pxSum=$pxSum%.1f"))
      }
  }

  private def report(startedNs: Long, n: Long, file: File, extra: String): Unit = {
    val seconds = (System.nanoTime() - startedNs) / 1e9
    val mb      = file.length / 1024.0 / 1024.0
    println(f"  $n%,d rows in $seconds%.2fs (${mb / seconds}%.1f MB/s, ${n / seconds}%,.0f rows/s)  $extra")
  }
}
