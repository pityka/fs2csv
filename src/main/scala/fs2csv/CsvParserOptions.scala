package fs2csv

final case class CsvParserOptions(
    fieldSeparator: Char = ',',
    quoteChar: Char = '"',
    recordSeparator: String = "\n",
    bufferSize: Int = 64 * 1024
) {
  def withSeparator(c: Char): CsvParserOptions  = copy(fieldSeparator = c)
  def withQuoteChar(c: Char): CsvParserOptions  = copy(quoteChar = c)
  def withRecordSeparator(s: String): CsvParserOptions = copy(recordSeparator = s)
  def withBufferSize(n: Int): CsvParserOptions  = copy(bufferSize = n)
}

object CsvParserOptions {
  val default: CsvParserOptions      = CsvParserOptions()
  val rfc4180: CsvParserOptions      = CsvParserOptions(recordSeparator = "\r\n")
  val tabSeparated: CsvParserOptions = CsvParserOptions(fieldSeparator = '\t')
}
