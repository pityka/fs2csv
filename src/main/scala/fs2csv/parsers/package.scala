package fs2csv

package object parsers {
  inline def parseInt(c: Array[Char], f: Int, t: Int): Int       = Numerics.parseInt(c, f, t)
  inline def parseLong(c: Array[Char], f: Int, t: Int): Long     = Numerics.parseLong(c, f, t)
  inline def parseShort(c: Array[Char], f: Int, t: Int): Short   = Numerics.parseShort(c, f, t)
  inline def parseByte(c: Array[Char], f: Int, t: Int): Byte     = Numerics.parseByte(c, f, t)
  inline def parseDouble(c: Array[Char], f: Int, t: Int): Double = Numerics.parseDouble(c, f, t)
  inline def parseBoolean(c: Array[Char], f: Int, t: Int): Boolean = Numerics.parseBoolean(c, f, t)
}
