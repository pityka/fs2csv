package fs2csv.bench

import java.io.{BufferedWriter, File, FileWriter}

import cats.effect.{IO, IOApp}
import fs2.io.file.{Files, Path}

import fs2csv.*

case class Trade(id: Long, name: String, score: Double, active: Boolean, note: String)
    derives RowReader, RowWriter

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

      encodeRows = sys.env.get("ENCODE_ROWS").map(_.toLong).getOrElse(rows)
      outFile    = {
        val f = new File(sys.env.getOrElse("CSV_OUT_PATH", "/tmp/fs2csv-bench-out.csv"))
        f.deleteOnExit(); f
      }
      _ <- IO(println(s"Encoding $encodeRows rows to $outFile ..."))
      _ <- IO(println("--- raw BufferedWriter (encode baseline, row-gen + file write) ---")) *>
             IO(benchRawBufferedWriterEncode(outFile, encodeRows))
      _ <- IO(println("--- CsvPipes encode → discard bytes (isolate encode CPU) ---")) *>
             benchCsvPipesEncodeDrain(encodeRows)
      _ <- IO(println("--- CsvPipes encode → Files[IO].writeAll (default) ---")) *>
             benchCsvPipesEncodeToFile(encodeRows, outFile)
      _ <- IO(println("--- CsvPipes encode → chunkN(4096) + Files[IO].writeAll ---")) *>
             benchCsvPipesEncodeChunkNWriteAll(encodeRows, outFile, 4096)
      _ <- IO(println("--- CsvPipes encode → chunkN(65536) + Files[IO].writeAll ---")) *>
             benchCsvPipesEncodeChunkNWriteAll(encodeRows, outFile, 65536)
      _ <- IO(println("--- CsvPipes encode → writeOutputStream(FileOutputStream) ---")) *>
             benchCsvPipesEncodeWriteOutputStream(encodeRows, outFile, buffered = false)
      _ <- IO(println("--- CsvPipes encode → writeOutputStream(BufferedOutputStream 64KiB) ---")) *>
             benchCsvPipesEncodeWriteOutputStream(encodeRows, outFile, buffered = true)
      _ <- IO(println("--- CsvPipes encode → FileChannel.write(ByteBuffer) ---")) *>
             benchCsvPipesEncodeFileChannel(encodeRows, outFile)
      _ <- IO(println("--- fs2-data-csv 1.14.1 encodeUsingFirstHeaders → utf8.encode → discard bytes ---")) *>
             benchFs2DataCsvEncodeDrain(encodeRows)
      _ <- IO(println("--- fs2-data-csv 1.14.1 encodeUsingFirstHeaders → utf8.encode → Files[IO].writeAll ---")) *>
             benchFs2DataCsvEncodeToFile(encodeRows, outFile)
      _ <- IO(println("--- fs2-data-csv 1.14.1 encodeUsingFirstHeaders → utf8.encode → writeOutputStream(BufferedOutputStream 64KiB) ---")) *>
             benchFs2DataCsvEncodeWriteOutputStream(encodeRows, outFile)
    } yield ()
  }

  private def tradeStream(n: Long): fs2.Stream[IO, Trade] =
    fs2.Stream.range(0L, n).map { i =>
      Trade(i, s"name_$i", (i % 1000) * 0.5, (i & 1L) == 0L, "hello world")
    }

  private def benchRawBufferedWriterEncode(file: File, n: Long): Unit = {
    val started = System.nanoTime()
    generateCsvFile(file, n)
    report(started, n, file, "raw BufferedWriter")
  }

  private def benchCsvPipesEncodeToFile(n: Long, out: File): IO[Unit] = {
    val opts    = CsvParserOptions.default.withBufferSize(1 << 18)
    val started = System.nanoTime()
    tradeStream(n)
      .through(CsvPipes.encodeUsingHeaders[IO, Trade](opts))
      .through(Files[IO].writeAll(Path.fromNioPath(out.toPath)))
      .compile
      .drain
      .flatMap(_ => IO(report(started, n, out, "encode → Files.writeAll")))
  }

  private def benchCsvPipesEncodeDrain(n: Long): IO[Unit] = {
    val opts    = CsvParserOptions.default.withBufferSize(1 << 18)
    val started = System.nanoTime()
    tradeStream(n)
      .through(CsvPipes.encodeUsingHeaders[IO, Trade](opts))
      .compile
      .count
      .flatMap(bytes => IO(reportBytes(started, n, bytes, "encode → discard")))
  }

  private def benchFs2DataCsvEncodeToFile(n: Long, out: File): IO[Unit] = {
    import fs2.data.csv.{encodeUsingFirstHeaders, CsvRowEncoder}
    import fs2.data.csv.generic.semiauto.deriveCsvRowEncoder
    given CsvRowEncoder[Trade, String] = deriveCsvRowEncoder[Trade]

    val started = System.nanoTime()
    tradeStream(n)
      .through(encodeUsingFirstHeaders[Trade]())
      .through(fs2.text.utf8.encode)
      .through(Files[IO].writeAll(Path.fromNioPath(out.toPath)))
      .compile
      .drain
      .flatMap(_ => IO(report(started, n, out, "fs2-data-csv encode → Files.writeAll")))
  }

  private def benchFs2DataCsvEncodeDrain(n: Long): IO[Unit] = {
    import fs2.data.csv.{encodeUsingFirstHeaders, CsvRowEncoder}
    import fs2.data.csv.generic.semiauto.deriveCsvRowEncoder
    given CsvRowEncoder[Trade, String] = deriveCsvRowEncoder[Trade]

    val started = System.nanoTime()
    tradeStream(n)
      .through(encodeUsingFirstHeaders[Trade]())
      .through(fs2.text.utf8.encode)
      .compile
      .count
      .flatMap(bytes => IO(reportBytes(started, n, bytes, "fs2-data-csv encode → discard")))
  }

  private def benchCsvPipesEncodeChunkNWriteAll(n: Long, out: File, rowsPerChunk: Int): IO[Unit] = {
    val opts    = CsvParserOptions.default.withBufferSize(1 << 18)
    val started = System.nanoTime()
    tradeStream(n)
      .chunkN(rowsPerChunk)
      .flatMap(fs2.Stream.chunk)
      .through(CsvPipes.encodeUsingHeaders[IO, Trade](opts))
      .through(Files[IO].writeAll(Path.fromNioPath(out.toPath)))
      .compile
      .drain
      .flatMap(_ => IO(report(started, n, out, s"chunkN($rowsPerChunk) + Files.writeAll")))
  }

  private def benchCsvPipesEncodeWriteOutputStream(n: Long, out: File, buffered: Boolean): IO[Unit] = {
    val opts       = CsvParserOptions.default.withBufferSize(1 << 18)
    val bufSize    = 1 << 16
    val outStream: IO[java.io.OutputStream] = IO {
      val fos = new java.io.FileOutputStream(out)
      if (buffered) new java.io.BufferedOutputStream(fos, bufSize) else fos
    }
    val label   = if (buffered) "writeOutputStream(BufferedOutputStream)" else "writeOutputStream(FileOutputStream)"
    val started = System.nanoTime()
    tradeStream(n)
      .through(CsvPipes.encodeUsingHeaders[IO, Trade](opts))
      .through(fs2.io.writeOutputStream[IO](outStream, closeAfterUse = true))
      .compile
      .drain
      .flatMap(_ => IO(report(started, n, out, label)))
  }

  private def benchCsvPipesEncodeFileChannel(n: Long, out: File): IO[Unit] = {
    val opts    = CsvParserOptions.default.withBufferSize(1 << 18)
    val started = System.nanoTime()
    fs2.Stream
      .bracket(IO(new java.io.FileOutputStream(out).getChannel))(ch => IO(ch.close()))
      .flatMap { ch =>
        tradeStream(n)
          .through(CsvPipes.encodeUsingHeaders[IO, Trade](opts))
          .chunks
          .evalMap { chunk =>
            IO {
              val bb = chunk match {
                case bbCh: fs2.Chunk.ByteBuffer => bbCh.buf.duplicate()
                case as: fs2.Chunk.ArraySlice[_] if as.values.isInstanceOf[Array[Byte]] =>
                  java.nio.ByteBuffer.wrap(
                    as.values.asInstanceOf[Array[Byte]],
                    as.offset,
                    as.size
                  )
                case _ =>
                  java.nio.ByteBuffer.wrap(chunk.toArray)
              }
              while (bb.hasRemaining) { val _ = ch.write(bb) }
            }
          }
      }
      .compile
      .drain
      .flatMap(_ => IO(report(started, n, out, "FileChannel.write(ByteBuffer)")))
  }

  private def benchFs2DataCsvEncodeWriteOutputStream(n: Long, out: File): IO[Unit] = {
    import fs2.data.csv.{encodeUsingFirstHeaders, CsvRowEncoder}
    import fs2.data.csv.generic.semiauto.deriveCsvRowEncoder
    given CsvRowEncoder[Trade, String] = deriveCsvRowEncoder[Trade]

    val bufSize    = 1 << 16
    val outStream: IO[java.io.OutputStream] = IO(
      new java.io.BufferedOutputStream(new java.io.FileOutputStream(out), bufSize)
    )
    val started = System.nanoTime()
    tradeStream(n)
      .through(encodeUsingFirstHeaders[Trade]())
      .through(fs2.text.utf8.encode)
      .through(fs2.io.writeOutputStream[IO](outStream, closeAfterUse = true))
      .compile
      .drain
      .flatMap(_ => IO(report(started, n, out, "fs2-data-csv encode → writeOutputStream(BufferedOutputStream)")))
  }

  private def reportBytes(startedNs: Long, n: Long, bytes: Long, extra: String): Unit = {
    val seconds = (System.nanoTime() - startedNs) / 1e9
    val mb      = bytes / 1024.0 / 1024.0
    println(f"  $n%,d rows in $seconds%.2fs (${mb / seconds}%.1f MB/s, ${n / seconds}%,.0f rows/s)  $extra")
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
