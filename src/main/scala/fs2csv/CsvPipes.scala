package fs2csv

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CharsetDecoder, CoderResult, CodingErrorAction}

import fs2.{Chunk, Pipe, Pull, RaiseThrowable, Stream}
import org.saddle.io.csv as saddle

final case class CsvDecodeError(message: String, line: Long, cause: Throwable = null)
    extends RuntimeException(s"line=$line: $message", cause)

object CsvPipes:

  def decodeWithoutHeaders[F[_]: RaiseThrowable, A](
      opts: CsvParserOptions = CsvParserOptions.default
  )(using r: RowReader[A]): Pipe[F, Byte, A] =
    bytesToRows[F, A](opts, headerAware = false, Naming.identity)

  def decodeUsingHeaders[F[_]: RaiseThrowable, A](
      opts: CsvParserOptions = CsvParserOptions.default,
      headerForField: String => String = Naming.identity
  )(using r: RowReader[A]): Pipe[F, Byte, A] =
    bytesToRows[F, A](opts, headerAware = true, headerForField)

  def encodeWithoutHeaders[F[_], A](
      opts: CsvParserOptions = CsvParserOptions.default
  )(using w: RowWriter[A]): Pipe[F, A, Byte] =
    rowsToBytes[F, A](opts, emitHeader = false, Naming.identity)

  def encodeUsingHeaders[F[_], A](
      opts: CsvParserOptions = CsvParserOptions.default,
      headerForField: String => String = Naming.identity
  )(using w: RowWriter[A]): Pipe[F, A, Byte] =
    rowsToBytes[F, A](opts, emitHeader = true, headerForField)

  private def rowsToBytes[F[_], A](
      opts:           CsvParserOptions,
      emitHeader:     Boolean,
      headerForField: String => String
  )(using w: RowWriter[A]): Pipe[F, A, Byte] = { in =>
    Stream.suspend {
      val drv = new RowEncodingDriver[A](opts, w)
      val prefix: Stream[F, Byte] =
        if (emitHeader) {
          drv.appendHeader(headerForField)
          Stream.chunk(drv.drainAsBytes())
        } else Stream.empty
      val body: Stream[F, Byte] =
        in.chunks.flatMap { chunk =>
          var i = 0
          while (i < chunk.size) {
            drv.appendRow(chunk(i))
            i += 1
          }
          Stream.chunk(drv.drainAsBytes())
        }
      prefix ++ body
    }
  }

  private final class RowEncodingDriver[A](
      opts:   CsvParserOptions,
      writer: RowWriter[A]
  ):
    private val fs     = opts.fieldSeparator
    private val qc     = opts.quoteChar
    private val rs     = opts.recordSeparator
    private val arity  = writer.arity
    private val rowSb  = new java.lang.StringBuilder(opts.bufferSize)
    private val cellSb = new java.lang.StringBuilder(64)

    def appendHeader(headerForField: String => String): Unit = {
      var i = 0
      while (i < arity) {
        if (i > 0) rowSb.append(fs)
        cellSb.setLength(0)
        cellSb.append(writer.headerName(i, headerForField))
        appendCellQuoted(cellSb)
        i += 1
      }
      rowSb.append(rs)
    }

    def appendRow(a: A): Unit = {
      var i = 0
      while (i < arity) {
        if (i > 0) rowSb.append(fs)
        cellSb.setLength(0)
        writer.encodeField(a, i, cellSb)
        appendCellQuoted(cellSb)
        i += 1
      }
      rowSb.append(rs)
    }

    def drainAsBytes(): Chunk[Byte] = {
      val len = rowSb.length
      if (len == 0) Chunk.empty
      else {
        val bytes = new Array[Byte](len)
        var i = 0
        while (i < len) {
          val c = rowSb.charAt(i)
          bytes(i) = (if (c > 127) '?' else c).toByte
          i += 1
        }
        rowSb.setLength(0)
        Chunk.array(bytes)
      }
    }

    private def appendCellQuoted(cell: java.lang.StringBuilder): Unit = {
      val len = cell.length
      var needsQuoting = false
      var i = 0
      while (i < len && !needsQuoting) {
        val c = cell.charAt(i)
        if (c == fs || c == qc || c == '\r' || c == '\n') needsQuoting = true
        i += 1
      }
      if (!needsQuoting) {
        rowSb.append(cell)
      } else {
        rowSb.append(qc)
        var j = 0
        while (j < len) {
          val c = cell.charAt(j)
          if (c == qc) { rowSb.append(qc); rowSb.append(qc) }
          else rowSb.append(c)
          j += 1
        }
        rowSb.append(qc)
      }
    }

  private def bytesToRows[F[_]: RaiseThrowable, A](
      opts: CsvParserOptions,
      headerAware: Boolean,
      headerForField: String => String
  )(using r: RowReader[A]): Pipe[F, Byte, A] = { in =>
    Stream.suspend {
      val driver = new ChunkDriver[A](opts, headerAware, headerForField, r)

      def loop(s: Stream[F, Byte]): Pull[F, A, Unit] =
        s.pull.uncons.flatMap {
          case Some((chunk, rest)) =>
            val rows = driver.feed(chunk, isEof = false)
            val continuation = driver.errorOption match {
              case None      => loop(rest)
              case Some(msg) => Pull.raiseError[F](new CsvDecodeError(msg, line = 0L))
            }
            if (rows.size > 0) Pull.output(rows) >> continuation else continuation
          case None =>
            val rows = driver.finish()
            driver.errorOption match {
              case None      => Pull.output(rows)
              case Some(msg) =>
                Pull.output(rows) >> Pull.raiseError[F](new CsvDecodeError(msg, line = 0L))
            }
        }

      loop(in).stream
    }
  }

  private final class ChunkDriver[A](
      opts:           CsvParserOptions,
      headerAware:    Boolean,
      headerForField: String => String,
      reader:         RowReader[A]
  ):
    private val bufferSize = opts.bufferSize
    private val charBuf    = CharBuffer.allocate(bufferSize)
    charBuf.limit(0)
    private val byteBuf       = ByteBuffer.allocate(bufferSize)
    private val charsetDecoder = newAsciiSilentDecoder()

    private final class ChannelToScanner extends Iterator[CharBuffer] {
      var nextBatchReady: Boolean = false
      def hasNext: Boolean        = nextBatchReady
      def next(): CharBuffer      = { nextBatchReady = false; charBuf }
    }
    private val charsToScanner = new ChannelToScanner

    private val saddleParser = {
      val rsep = opts.recordSeparator
      if (rsep.length == 2)
        org.saddle.io.csv.SaddleBridge.newPairSep(
          charsToScanner, opts.quoteChar, opts.fieldSeparator,
          rsep.charAt(0), rsep.charAt(1), bufferSize
        )
      else
        org.saddle.io.csv.SaddleBridge.newSingleSep(
          charsToScanner, opts.quoteChar, opts.fieldSeparator,
          rsep.charAt(0), bufferSize
        )
    }

    private val pendingRows = scala.collection.mutable.ArrayBuffer.empty[A]
    private val rowAssembler = new RowAssemblingCallback[A](
      reader,
      a => { pendingRows += a; true },
      if (headerAware) HeaderToFieldIndex.placeholder else null,
      headerForField
    )

    private var errMsg: String = null

    def errorOption: Option[String] = Option(errMsg)

    def feed(chunk: Chunk[Byte], isEof: Boolean): Chunk[A] = {
      if (errMsg != null) return Chunk.empty
      val total = chunk.size
      var off   = 0
      while (off < total && errMsg == null) {
        val byteRoom = byteBuf.capacity - byteBuf.position
        val take     = math.min(byteRoom, total - off)
        copyChunkInto(chunk, off, take, byteBuf)
        off += take
        if (byteBuf.position == byteBuf.capacity) runOneScanBatch(eofForDecoder = false)
      }
      if (!isEof) runOneScanBatch(eofForDecoder = false)
      drainPendingRows()
    }

    def finish(): Chunk[A] = {
      if (errMsg != null) return Chunk.empty
      runOneScanBatch(eofForDecoder = true)
      val (chars, from, to, len, eol) =
        org.saddle.io.csv.SaddleBridge.emitRest(saddleParser)
      if (chars != null && errMsg == null)
        rowAssembler(chars, from, to, len, eol) match {
          case org.saddle.io.csv.Error(m) => errMsg = m
          case _                          => ()
        }
      if (errMsg == null) rowAssembler.flushPending() match {
        case Some(m) => errMsg = m
        case None    => ()
      }
      if (errMsg == null && org.saddle.io.csv.SaddleBridge.bufferTooShort(saddleParser))
        errMsg = s"row exceeded bufferSize=$bufferSize; increase bufferSize"
      if (errMsg == null && rowAssembler.errorMessage.isDefined)
        errMsg = rowAssembler.errorMessage.get
      drainPendingRows()
    }

    private def runOneScanBatch(eofForDecoder: Boolean): Unit = {
      charBuf.compact()
      byteBuf.flip()
      val result = charsetDecoder.decode(byteBuf, charBuf, eofForDecoder)
      if (result.isError) {
        errMsg = s"decoder error: $result"
        byteBuf.compact()
        charBuf.flip()
        return
      }
      byteBuf.compact()
      if (eofForDecoder) {
        val _: CoderResult = charsetDecoder.flush(charBuf)
      }
      charBuf.flip()

      if (!charBuf.hasRemaining) return

      charsToScanner.nextBatchReady = true
      if (org.saddle.io.csv.SaddleBridge.hasNext(saddleParser)) {
        val (chars, from, to, len, eol) =
          org.saddle.io.csv.SaddleBridge.nextBatch(saddleParser)
        if (len < 0) {
          errMsg = "unclosed quote or other parse error"
          return
        }
        rowAssembler(chars, from, to, len, eol) match {
          case org.saddle.io.csv.Error(m) => errMsg = m
          case _                          => ()
        }
      }
    }

    private def drainPendingRows(): Chunk[A] = {
      if (pendingRows.isEmpty) Chunk.empty
      else {
        val out = Chunk.from(pendingRows.toVector)
        pendingRows.clear()
        out
      }
    }

    private def copyChunkInto(chunk: Chunk[Byte], off: Int, n: Int, dst: ByteBuffer): Unit = {
      chunk match {
        case bb: Chunk.ByteBuffer =>
          val src      = bb.buf.duplicate()
          val srcStart = bb.offset + off
          src.position(srcStart)
          src.limit(srcStart + n)
          dst.put(src)

        case as: Chunk.ArraySlice[_] if as.values.isInstanceOf[Array[Byte]] =>
          dst.put(as.values.asInstanceOf[Array[Byte]], as.offset + off, n)

        case _ =>
          var i = 0
          while (i < n) { dst.put(chunk(off + i)); i += 1 }
      }
    }

  private def newAsciiSilentDecoder(): CharsetDecoder =
    Charset
      .forName("US-ASCII")
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE)

private[fs2csv] final class HeaderToFieldIndex(val csvColumnToCaseClassField: Array[Int])

private[fs2csv] object HeaderToFieldIndex {
  val placeholder: HeaderToFieldIndex = new HeaderToFieldIndex(null)
}

private[fs2csv] final class RowAssemblingCallback[A](
    reader:                  RowReader[A],
    consume:                 A => Boolean,
    private var headerIndex: HeaderToFieldIndex,
    headerForField:          String => String
) extends saddle.Callback {

  private val arity              = reader.arity
  private val headerAware        = headerIndex != null
  private var parsingHeaderRow   = headerAware
  private val collectedHeaders   =
    if (headerAware) new java.util.ArrayList[String](arity * 2) else null
  private var currentCsvColumn   = 0
  private var lineNumber         = 0L
  private var errMsg: String     = null
  private var rowInProgress      = false
  private var consumerSaidStop   = false

  def errorMessage: Option[String] = Option(errMsg)

  private def expectedRowWidth(): Int =
    if (headerIndex == null) arity
    else headerIndex.csvColumnToCaseClassField.length

  private def shortRowMessage(): String =
    s"line=${lineNumber + 1}: expected ${expectedRowWidth()} columns, got $currentCsvColumn"

  def flushPending(): Option[String] = {
    if (errMsg != null || consumerSaidStop || parsingHeaderRow || !rowInProgress)
      return Option(errMsg)
    if (currentCsvColumn < expectedRowWidth()) {
      val msg = shortRowMessage()
      errMsg = msg
      return Some(msg)
    }
    try {
      val a = reader.build()
      reader.reset()
      rowInProgress = false
      currentCsvColumn = 0
      lineNumber += 1L
      if (!consume(a)) consumerSaidStop = true
      None
    } catch {
      case e: Throwable =>
        val msg = s"line=${lineNumber + 1}: ${e.getMessage}"
        errMsg = msg
        Some(msg)
    }
  }

  override def apply(
      chars: Array[Char],
      from:  Array[Int],
      to:    Array[Int],
      len:   Int,
      eol:   Array[Int]
  ): saddle.Control = {
    if (len < 0) {
      errMsg = "unclosed quote"
      return saddle.Error("unclosed quote")
    }
    try {
      var i    = 0
      var stop = false
      while (i < len && !stop) {
        val cellFrom    = from(i)
        val rawCellTo   = to(i)
        val cellTo      = if (rawCellTo < 0) -rawCellTo else rawCellTo
        val cellClosesRow = eol(i) == -1
        if (parsingHeaderRow) {
          collectedHeaders.add(new String(chars, cellFrom, cellTo - cellFrom))
          if (cellClosesRow) {
            headerIndex = buildHeaderIndex()
            parsingHeaderRow = false
            currentCsvColumn = 0
            lineNumber = 1L
          }
        } else {
          val fieldIdx = caseClassFieldForCsvColumn(currentCsvColumn)
          if (fieldIdx >= 0) reader.setField(fieldIdx, chars, cellFrom, cellTo)
          currentCsvColumn += 1
          rowInProgress = true
          if (cellClosesRow) {
            if (currentCsvColumn < expectedRowWidth()) {
              val msg = shortRowMessage()
              errMsg = msg
              return saddle.Error(msg)
            }
            val a = reader.build()
            reader.reset()
            currentCsvColumn = 0
            rowInProgress = false
            lineNumber += 1L
            if (!consume(a)) { consumerSaidStop = true; stop = true }
          }
        }
        i += 1
      }
      if (stop) saddle.Done else saddle.Next
    } catch {
      case e: Throwable =>
        val msg = s"line=${if (parsingHeaderRow) 1 else lineNumber + 1}: ${e.getMessage}"
        errMsg = msg
        saddle.Error(msg)
    }
  }

  private def caseClassFieldForCsvColumn(csvCol: Int): Int =
    if (headerIndex == null) {
      if (csvCol < arity) csvCol else -1
    } else {
      val table = headerIndex.csvColumnToCaseClassField
      if (csvCol < table.length) table(csvCol) else -1
    }

  private def buildHeaderIndex(): HeaderToFieldIndex = {
    val labels   = reader.fieldLabels
    val fieldCount = labels.length
    val csvWidth = collectedHeaders.size

    val expectedHeaderForField = new Array[String](fieldCount)
    var i = 0
    while (i < fieldCount) {
      expectedHeaderForField(i) = reader.headerName(i, headerForField)
      i += 1
    }

    val csvColumnToField = new Array[Int](csvWidth)
    var col = 0
    while (col < csvWidth) {
      val headerName = collectedHeaders.get(col)
      var matchedField = -1
      var j = 0
      while (j < fieldCount && matchedField < 0) {
        if (expectedHeaderForField(j) == headerName) matchedField = j
        j += 1
      }
      csvColumnToField(col) = matchedField
      col += 1
    }

    val fieldSeen = new Array[Boolean](fieldCount)
    var k = 0
    while (k < csvWidth) {
      val f = csvColumnToField(k)
      if (f >= 0) fieldSeen(f) = true
      k += 1
    }
    var jf = 0
    while (jf < fieldCount) {
      if (!fieldSeen(jf))
        throw new IllegalArgumentException(
          s"CSV header is missing column for case-class field '${labels(jf)}' " +
            s"(expected header '${expectedHeaderForField(jf)}', saw: ${collectedHeaders.toString})"
        )
      jf += 1
    }
    new HeaderToFieldIndex(csvColumnToField)
  }
}
