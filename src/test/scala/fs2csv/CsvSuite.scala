package fs2csv

import java.io.{File, FileWriter}

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import munit.{CatsEffectSuite, FunSuite}

class CsvSuite extends FunSuite {

  private def decodeStringSynchronously[A: RowReader](
      input:        String,
      withHeaders:  Boolean = false,
      opts:         CsvParserOptions = CsvParserOptions.default
  ): List[A] = {
    val source = Stream.chunk(fs2.Chunk.array(input.getBytes("US-ASCII"))).covary[IO]
    val pipe   =
      if (withHeaders) CsvPipes.decodeUsingHeaders[IO, A](opts)
      else CsvPipes.decodeWithoutHeaders[IO, A](opts)
    source.through(pipe).compile.toList.unsafeRunSync()
  }

  case class IdName(id: Int, name: String) derives RowReader

  test("positional decode: int + string") {
    val out = decodeStringSynchronously[IdName]("1,alice\n2,bob\n")
    assertEquals(out, List(IdName(1, "alice"), IdName(2, "bob")))
  }

  test("trailing row without newline") {
    val out = decodeStringSynchronously[IdName]("1,alice\n2,bob")
    assertEquals(out, List(IdName(1, "alice"), IdName(2, "bob")))
  }

  case class Trade(id: Long, sym: String, qty: Int, px: Double) derives RowReader

  test("mixed types — no String allocated for primitives") {
    val out = decodeStringSynchronously[Trade]("1,AAPL,100,189.42\n2,GOOG,50,170.5\n")
    assertEquals(out, List(Trade(1L, "AAPL", 100, 189.42), Trade(2L, "GOOG", 50, 170.5)))
  }

  test("boolean (true/false/1/0/yes/no)") {
    case class Flag(name: String, on: Boolean) derives RowReader
    val out = decodeStringSynchronously[Flag]("a,true\nb,false\nc,1\nd,0\ne,YES\nf,no\n")
    assertEquals(out.map(_.on), List(true, false, true, false, true, false))
  }

  test("Option[A] — empty cell becomes None") {
    case class Maybe(id: Int, label: Option[String], n: Option[Long]) derives RowReader
    val out = decodeStringSynchronously[Maybe]("1,foo,42\n2,,\n3,bar,7\n")
    assertEquals(
      out,
      List(Maybe(1, Some("foo"), Some(42L)), Maybe(2, None, None), Maybe(3, Some("bar"), Some(7L)))
    )
  }

  test("empty cells (present but empty) decode normally for String and Option") {
    case class Row(a: String, b: Option[Int], c: String) derives RowReader
    val out = decodeStringSynchronously[Row]("x,,z\n")
    assertEquals(out, List(Row("x", None, "z")))
  }

  test("missing columns in headerless row crash cleanly (not NPE)") {
    case class Row(a: String, b: Option[Int], c: String) derives RowReader
    val source = Stream
      .chunk(fs2.Chunk.array("x\n".getBytes("US-ASCII")))
      .covary[IO]
    val outcome = source
      .through(CsvPipes.decodeWithoutHeaders[IO, Row]())
      .compile
      .toList
      .attempt
      .unsafeRunSync()
    assert(outcome.isLeft, s"expected an error, got $outcome")
    val msg = outcome.left.toOption.get.getMessage
    assert(msg.contains("expected 3") && msg.contains("got 1"), msg)
  }

  test("missing columns in trailing (no-newline) headerless row crash cleanly") {
    case class Row(a: String, b: String, c: String) derives RowReader
    val source = Stream
      .chunk(fs2.Chunk.array("a,b,c\nx".getBytes("US-ASCII")))
      .covary[IO]
    val outcome = source
      .through(CsvPipes.decodeWithoutHeaders[IO, Row]())
      .compile
      .toList
      .attempt
      .unsafeRunSync()
    assert(outcome.isLeft, s"expected an error, got $outcome")
    val msg = outcome.left.toOption.get.getMessage
    assert(msg.contains("expected 3") && msg.contains("got 1"), msg)
  }

  test("missing columns in header-aware row crash cleanly (not NPE)") {
    case class Row(name: String, age: Option[Int], note: Option[String]) derives RowReader
    val source = Stream
      .chunk(fs2.Chunk.array("name,age,note\nalice\n".getBytes("US-ASCII")))
      .covary[IO]
    val outcome = source
      .through(CsvPipes.decodeUsingHeaders[IO, Row]())
      .compile
      .toList
      .attempt
      .unsafeRunSync()
    assert(outcome.isLeft, s"expected an error, got $outcome")
    val msg = outcome.left.toOption.get.getMessage
    assert(msg.contains("expected 3") && msg.contains("got 1"), msg)
  }

  test("header-aware: columns are reordered to match case-class fields") {
    case class Person(name: String, age: Int) derives RowReader
    val out = decodeStringSynchronously[Person]("age,name\n30,alice\n25,bob\n", withHeaders = true)
    assertEquals(out, List(Person("alice", 30), Person("bob", 25)))
  }

  test("header-aware: extra CSV columns are ignored") {
    case class Person(name: String, age: Int) derives RowReader
    val out = decodeStringSynchronously[Person](
      "name,extra,age\nalice,foo,30\nbob,bar,25\n",
      withHeaders = true
    )
    assertEquals(out, List(Person("alice", 30), Person("bob", 25)))
  }

  test("header-aware: case class as a strict subset of CSV columns") {
    case class IdScore(id: Long, score: Double) derives RowReader
    val csv = "id,name,score,active,note\n1,alice,1.5,true,hi\n2,bob,2.5,false,bye\n"
    val out = decodeStringSynchronously[IdScore](csv, withHeaders = true)
    assertEquals(out, List(IdScore(1L, 1.5), IdScore(2L, 2.5)))
  }

  test("header-aware: @csvName annotation renames the lookup") {
    case class P(
        @csvName("person_name") name: String,
        @csvName("person_age")  age:  Int
    ) derives RowReader
    val out = decodeStringSynchronously[P]("person_name,person_age\nalice,30\nbob,25\n", withHeaders = true)
    assertEquals(out, List(P("alice", 30), P("bob", 25)))
  }

  test("header-aware: snake_case Naming transform") {
    case class P(personName: String, personAge: Int) derives RowReader
    val out = Stream
      .chunk(fs2.Chunk.array("person_name,person_age\nalice,30\nbob,25\n".getBytes("US-ASCII")))
      .covary[IO]
      .through(CsvPipes.decodeUsingHeaders[IO, P](headerForField = Naming.snakeCase))
      .compile
      .toList
      .unsafeRunSync()
    assertEquals(out, List(P("alice", 30), P("bob", 25)))
  }

  test("header-aware: explicit Map override via Naming.fromMap") {
    case class P(name: String, age: Int) derives RowReader
    val mapping = Naming.fromMap(Map("name" -> "n", "age" -> "a"))
    val out = Stream
      .chunk(fs2.Chunk.array("n,a\nalice,30\nbob,25\n".getBytes("US-ASCII")))
      .covary[IO]
      .through(CsvPipes.decodeUsingHeaders[IO, P](headerForField = mapping))
      .compile
      .toList
      .unsafeRunSync()
    assertEquals(out, List(P("alice", 30), P("bob", 25)))
  }

  test("@csvName wins over Naming transform") {
    case class P(@csvName("EXACT_NAME") name: String, age: Int) derives RowReader
    val out = Stream
      .chunk(fs2.Chunk.array("EXACT_NAME,age\nalice,30\n".getBytes("US-ASCII")))
      .covary[IO]
      .through(CsvPipes.decodeUsingHeaders[IO, P](headerForField = Naming.snakeCase))
      .compile
      .toList
      .unsafeRunSync()
    assertEquals(out, List(P("alice", 30)))
  }

  test("header-aware: missing case-class field in header → error") {
    case class Person(name: String, age: Int) derives RowReader
    val source = Stream
      .chunk(fs2.Chunk.array("name,city\nalice,NYC\n".getBytes("US-ASCII")))
      .covary[IO]
    val outcome = source
      .through(CsvPipes.decodeUsingHeaders[IO, Person]())
      .compile
      .toList
      .attempt
      .unsafeRunSync()
    assert(outcome.isLeft, s"expected an error, got $outcome")
    val msg = outcome.left.toOption.get.getMessage
    assert(msg.contains("age"), s"error should mention 'age': $msg")
  }

  test("tab separated") {
    val out = decodeStringSynchronously[IdName]("1\talice\n2\tbob\n", opts = CsvParserOptions.tabSeparated)
    assertEquals(out, List(IdName(1, "alice"), IdName(2, "bob")))
  }

  test("CRLF line endings (RFC 4180)") {
    val out = decodeStringSynchronously[IdName]("1,alice\r\n2,bob\r\n", opts = CsvParserOptions.rfc4180)
    assertEquals(out, List(IdName(1, "alice"), IdName(2, "bob")))
  }

  test("quoted cell containing the separator and escaped quotes") {
    val csv =
      """1,"hello, world"
        |2,"with ""quotes"" inside"
        |""".stripMargin
    val out = decodeStringSynchronously[IdName](csv)
    assertEquals(out, List(IdName(1, "hello, world"), IdName(2, """with "quotes" inside""")))
  }

  test("Numerics.parseDouble matches Double.parseDouble for common formats") {
    val cases = List(
      "0", "1", "-1", "3.14", "-3.14", "1.5e2", "2.5E-3",
      "1.23456789012345", "1e10", "-1.0e-5"
    )
    cases.foreach { s =>
      val chars = s.toCharArray
      val a     = parsers.parseDouble(chars, 0, chars.length)
      val b     = java.lang.Double.parseDouble(s)
      assert(a == b, s"$s → ours=$a vs java=$b")
    }
  }

  test("Numerics.parseLong matches Long.parseLong for boundary values") {
    val cases = List("0", "1", "-1", "9223372036854775807", "-9223372036854775808", "12345678901234")
    cases.foreach { s =>
      val chars = s.toCharArray
      val a     = parsers.parseLong(chars, 0, chars.length)
      val b     = java.lang.Long.parseLong(s)
      assert(a == b, s"$s → ours=$a vs java=$b")
    }
  }

  test("end-to-end: temp file → fs2 pipe → row count") {
    val tmp = File.createTempFile("fs2csv-test", ".csv")
    tmp.deleteOnExit()
    val w = new FileWriter(tmp)
    try {
      w.write("id,name\n")
      var i = 0
      while (i < 5000) { w.write(s"$i,name_$i\n"); i += 1 }
    } finally w.close()

    val count = fs2.io.file
      .Files[IO]
      .readAll(fs2.io.file.Path.fromNioPath(tmp.toPath))
      .through(CsvPipes.decodeUsingHeaders[IO, IdName]())
      .compile
      .count
      .unsafeRunSync()
    assertEquals(count, 5000L)
  }
}

class CsvPipesSuite extends CatsEffectSuite {

  case class IdName(id: Int, name: String) derives RowReader

  test("fs2 Pipe — decodeWithoutHeaders streams rows") {
    val csv = "1,alice\n2,bob\n3,carol\n"
    Stream
      .emits(csv.getBytes("US-ASCII").toIndexedSeq)
      .covary[IO]
      .through(CsvPipes.decodeWithoutHeaders[IO, IdName]())
      .compile
      .toList
      .map { decoded =>
        assertEquals(decoded, List(IdName(1, "alice"), IdName(2, "bob"), IdName(3, "carol")))
      }
  }

  test("fs2 Pipe — decodeUsingHeaders reorders columns") {
    case class P(name: String, age: Int) derives RowReader
    val csv = "age,name\n30,alice\n25,bob\n"
    Stream
      .emits(csv.getBytes("US-ASCII").toIndexedSeq)
      .covary[IO]
      .through(CsvPipes.decodeUsingHeaders[IO, P]())
      .compile
      .toList
      .map { decoded => assertEquals(decoded, List(P("alice", 30), P("bob", 25))) }
  }

  test("fs2 Pipe — Chunk.ByteBuffer fast path") {
    val csv = "1,alice\n2,bob\n3,carol\n"
    val bb  = java.nio.ByteBuffer.wrap(csv.getBytes("US-ASCII"))
    Stream
      .chunk(fs2.Chunk.ByteBuffer(bb))
      .covary[IO]
      .through(CsvPipes.decodeWithoutHeaders[IO, IdName]())
      .compile
      .toList
      .map { decoded =>
        assertEquals(decoded, List(IdName(1, "alice"), IdName(2, "bob"), IdName(3, "carol")))
      }
  }

  test("fs2 Pipe — small input chunks straddling row boundaries") {
    val rowCount = 5_000
    val sb = new StringBuilder()
    sb.append("id,name\n")
    var i = 0
    while (i < rowCount) { sb.append(i).append(",n").append(i).append('\n'); i += 1 }
    Stream
      .emits(sb.toString.getBytes("US-ASCII").toIndexedSeq)
      .chunkN(7)
      .flatMap(Stream.chunk)
      .covary[IO]
      .through(CsvPipes.decodeUsingHeaders[IO, IdName]())
      .compile
      .count
      .map(n => assertEquals(n, rowCount.toLong))
  }
}

class CsvEncodeSuite extends FunSuite {

  private def encodeToString[A: RowWriter](
      rows:        List[A],
      withHeaders: Boolean = false,
      opts:        CsvParserOptions = CsvParserOptions.default,
      naming:      String => String = Naming.identity
  ): String = {
    val pipe =
      if (withHeaders) CsvPipes.encodeUsingHeaders[IO, A](opts, naming)
      else CsvPipes.encodeWithoutHeaders[IO, A](opts)
    Stream
      .emits(rows)
      .covary[IO]
      .through(pipe)
      .compile
      .to(Array)
      .map(bs => new String(bs, "US-ASCII"))
      .unsafeRunSync()
  }

  case class IdName(id: Int, name: String) derives RowReader, RowWriter

  test("encode without headers: primitive + string") {
    val out = encodeToString(List(IdName(1, "alice"), IdName(2, "bob")))
    assertEquals(out, "1,alice\n2,bob\n")
  }

  test("encode with headers uses field names") {
    val out = encodeToString(List(IdName(1, "alice")), withHeaders = true)
    assertEquals(out, "id,name\n1,alice\n")
  }

  case class Trade(id: Long, sym: String, qty: Int, px: Double, active: Boolean)
      derives RowWriter

  test("mixed types encode") {
    val out = encodeToString(List(Trade(1L, "AAPL", 100, 189.42, true)))
    assertEquals(out, "1,AAPL,100,189.42,true\n")
  }

  case class Cell(id: Int, note: String) derives RowWriter

  test("quotes cells that contain the field separator") {
    val out = encodeToString(List(Cell(1, "hello, world")))
    assertEquals(out, "1,\"hello, world\"\n")
  }

  test("quotes cells with embedded quotes and doubles them") {
    val out = encodeToString(List(Cell(1, "with \"quotes\" inside")))
    assertEquals(out, "1,\"with \"\"quotes\"\" inside\"\n")
  }

  test("quotes cells containing CR or LF") {
    val out = encodeToString(List(Cell(1, "line1\nline2")))
    assertEquals(out, "1,\"line1\nline2\"\n")
  }

  case class Maybe(id: Int, label: Option[String], n: Option[Long]) derives RowWriter

  test("Option[A]: None → empty cell, Some(v) → value") {
    val out = encodeToString(
      List(Maybe(1, Some("foo"), Some(42L)), Maybe(2, None, None), Maybe(3, Some("bar"), Some(7L)))
    )
    assertEquals(out, "1,foo,42\n2,,\n3,bar,7\n")
  }

  test("CRLF record separator") {
    val out = encodeToString(
      List(IdName(1, "alice"), IdName(2, "bob")),
      opts = CsvParserOptions.rfc4180
    )
    assertEquals(out, "1,alice\r\n2,bob\r\n")
  }

  test("tab separated") {
    val out = encodeToString(
      List(IdName(1, "alice"), IdName(2, "bob")),
      opts = CsvParserOptions.tabSeparated
    )
    assertEquals(out, "1\talice\n2\tbob\n")
  }

  test("@csvName renames the header") {
    case class P(
        @csvName("person_name") name: String,
        @csvName("person_age")  age:  Int
    ) derives RowWriter
    val out = encodeToString(List(P("alice", 30)), withHeaders = true)
    assertEquals(out, "person_name,person_age\nalice,30\n")
  }

  test("Naming.snakeCase transforms field names") {
    case class P(personName: String, personAge: Int) derives RowWriter
    val out = encodeToString(
      List(P("alice", 30)),
      withHeaders = true,
      naming = Naming.snakeCase
    )
    assertEquals(out, "person_name,person_age\nalice,30\n")
  }

  test("@csvName wins over Naming transform") {
    case class P(@csvName("EXACT_NAME") name: String, age: Int) derives RowWriter
    val out = encodeToString(
      List(P("alice", 30)),
      withHeaders = true,
      naming = Naming.snakeCase
    )
    assertEquals(out, "EXACT_NAME,age\nalice,30\n")
  }

  test("round trip decode → encode → decode") {
    val input = "id,name\n1,alice\n2,bob\n3,\"carol, jr\"\n"
    val decoded = Stream
      .chunk(fs2.Chunk.array(input.getBytes("US-ASCII")))
      .covary[IO]
      .through(CsvPipes.decodeUsingHeaders[IO, IdName]())
      .compile
      .toList
      .unsafeRunSync()

    val encoded = encodeToString(decoded, withHeaders = true)

    val redecoded = Stream
      .chunk(fs2.Chunk.array(encoded.getBytes("US-ASCII")))
      .covary[IO]
      .through(CsvPipes.decodeUsingHeaders[IO, IdName]())
      .compile
      .toList
      .unsafeRunSync()

    assertEquals(redecoded, decoded)
  }
}
