package fs2csv.parsers

import org.saddle.scalar.{
  ScalarTagByte,
  ScalarTagDouble,
  ScalarTagInt,
  ScalarTagLong,
  ScalarTagShort
}

object Numerics {

  inline def parseInt(c: Array[Char], from: Int, until: Int): Int =
    ScalarTagInt.parse(c, from, until)

  inline def parseLong(c: Array[Char], from: Int, until: Int): Long =
    ScalarTagLong.parse(c, from, until)

  inline def parseShort(c: Array[Char], from: Int, until: Int): Short =
    ScalarTagShort.parse(c, from, until)

  inline def parseByte(c: Array[Char], from: Int, until: Int): Byte =
    ScalarTagByte.parse(c, from, until)

  inline def parseDouble(c: Array[Char], from: Int, until: Int): Double =
    ScalarTagDouble.parse(c, from, until)

  final def parseBoolean(s: Array[Char], from: Int, to: Int): Boolean = {
    val len = to - from
    if (len == 4 &&
        (s(from) | 0x20) == 't' &&
        (s(from + 1) | 0x20) == 'r' &&
        (s(from + 2) | 0x20) == 'u' &&
        (s(from + 3) | 0x20) == 'e') true
    else if (len == 5 &&
             (s(from) | 0x20) == 'f' &&
             (s(from + 1) | 0x20) == 'a' &&
             (s(from + 2) | 0x20) == 'l' &&
             (s(from + 3) | 0x20) == 's' &&
             (s(from + 4) | 0x20) == 'e') false
    else if (len == 1) {
      val ch = s(from) | 0x20
      if (ch == 't' || ch == 'y' || ch == '1') true
      else if (ch == 'f' || ch == 'n' || ch == '0') false
      else throw notABoolean(s, from, len)
    } else if (len == 3 &&
               (s(from) | 0x20) == 'y' &&
               (s(from + 1) | 0x20) == 'e' &&
               (s(from + 2) | 0x20) == 's') true
    else if (len == 2 &&
             (s(from) | 0x20) == 'n' &&
             (s(from + 1) | 0x20) == 'o') false
    else throw notABoolean(s, from, len)
  }

  private def notABoolean(s: Array[Char], from: Int, len: Int): NumberFormatException =
    new NumberFormatException(s"not a boolean: '${new String(s, from, len)}'")
}
