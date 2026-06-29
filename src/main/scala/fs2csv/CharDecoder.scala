package fs2csv

trait CharDecoder[A]:
  def decode(chars: Array[Char], from: Int, until: Int): A

object CharDecoder:

  inline def apply[A](using d: CharDecoder[A]): CharDecoder[A] = d

  given CharDecoder[String] = (c, f, t) => unescapeDoubledQuotes(c, f, t)

  private def unescapeDoubledQuotes(c: Array[Char], f: Int, t: Int): String = {
    var i = f
    var hasDoubledQuote = false
    while (i < t - 1) {
      if (c(i) == '"' && c(i + 1) == '"') { hasDoubledQuote = true; i = t }
      else i += 1
    }
    if (!hasDoubledQuote) new String(c, f, t - f)
    else {
      val sb = new java.lang.StringBuilder(t - f)
      var j  = f
      while (j < t) {
        if (j + 1 < t && c(j) == '"' && c(j + 1) == '"') { sb.append('"'); j += 2 }
        else { sb.append(c(j)); j += 1 }
      }
      sb.toString
    }
  }

  given CharDecoder[Int] = (c, f, t) => {
    val v = parsers.Numerics.parseInt(c, f, t)
    if (v == Int.MinValue && !isLiteralMinInt(c, f, t)) throw notA("Int", c, f, t)
    else v
  }

  given CharDecoder[Long] = (c, f, t) => {
    val v = parsers.Numerics.parseLong(c, f, t)
    if (v == Long.MinValue && !isLiteralMinLong(c, f, t)) throw notA("Long", c, f, t)
    else v
  }

  given CharDecoder[Short] = (c, f, t) => {
    val v = parsers.Numerics.parseShort(c, f, t)
    if (v == Short.MinValue && !isLiteralMinShort(c, f, t)) throw notA("Short", c, f, t)
    else v
  }

  given CharDecoder[Byte] = (c, f, t) => {
    val v = parsers.Numerics.parseByte(c, f, t)
    if (v == Byte.MinValue && !isLiteralMinByte(c, f, t)) throw notA("Byte", c, f, t)
    else v
  }

  given CharDecoder[Double] = (c, f, t) => parsers.Numerics.parseDouble(c, f, t)

  given CharDecoder[Float] = (c, f, t) => parsers.Numerics.parseDouble(c, f, t).toFloat

  given CharDecoder[Boolean] = (c, f, t) => parsers.Numerics.parseBoolean(c, f, t)

  given CharDecoder[Char] = (c, f, t) =>
    if (t - f == 1) c(f)
    else throw new IllegalArgumentException(
      s"expected single char, got '${new String(c, f, t - f)}'"
    )

  given CharDecoder[BigInt]     = (c, f, t) => BigInt(new String(c, f, t - f))
  given CharDecoder[BigDecimal] = (c, f, t) => BigDecimal(new String(c, f, t - f))

  given option[A](using d: CharDecoder[A]): CharDecoder[Option[A]] =
    (c, f, t) => if (t == f) None else Some(d.decode(c, f, t))

  private def notA(typ: String, c: Array[Char], f: Int, t: Int): NumberFormatException =
    new NumberFormatException(s"not a $typ: '${new String(c, f, t - f)}'")

  private def isLiteralMinInt(c: Array[Char], f: Int, t: Int): Boolean = {
    val len = t - f
    len == 11 &&
    c(f) == '-' && c(f + 1) == '2' && c(f + 2) == '1' && c(f + 3) == '4' &&
    c(f + 4) == '7' && c(f + 5) == '4' && c(f + 6) == '8' && c(f + 7) == '3' &&
    c(f + 8) == '6' && c(f + 9) == '4' && c(f + 10) == '8'
  }

  private def isLiteralMinLong(c: Array[Char], f: Int, t: Int): Boolean = {
    val len = t - f
    if (len != 20) return false
    val expected = "-9223372036854775808"
    var i = 0
    while (i < 20) { if (c(f + i) != expected.charAt(i)) return false; i += 1 }
    true
  }

  private def isLiteralMinShort(c: Array[Char], f: Int, t: Int): Boolean = {
    val len = t - f
    if (len != 6) return false
    val expected = "-32768"
    var i = 0
    while (i < 6) { if (c(f + i) != expected.charAt(i)) return false; i += 1 }
    true
  }

  private def isLiteralMinByte(c: Array[Char], f: Int, t: Int): Boolean = {
    val len = t - f
    if (len != 4) return false
    val expected = "-128"
    var i = 0
    while (i < 4) { if (c(f + i) != expected.charAt(i)) return false; i += 1 }
    true
  }
