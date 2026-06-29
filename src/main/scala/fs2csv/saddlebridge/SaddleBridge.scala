package org.saddle.io.csv

import java.nio.CharBuffer

object SaddleBridge {

  type DBuf = DataBuffer

  def newSingleSep(
      data: Iterator[CharBuffer],
      quoteChar: Char,
      fieldSeparator: Char,
      recordSeparator: Char,
      bufferSize: Int
  ): DBuf =
    new DataBuffer1(data, quoteChar, fieldSeparator, recordSeparator, bufferSize)

  def newPairSep(
      data: Iterator[CharBuffer],
      quoteChar: Char,
      fieldSeparator: Char,
      rsep1: Char,
      rsep2: Char,
      bufferSize: Int
  ): DBuf =
    new DataBuffer2(data, quoteChar, fieldSeparator, rsep1, rsep2, bufferSize)

  inline def hasNext(db: DBuf): Boolean         = db.hasNext
  inline def bufferTooShort(db: DBuf): Boolean  = db.bufferTooShort
  inline def nextBatch(db: DBuf): (Array[Char], Array[Int], Array[Int], Int, Array[Int]) =
    db.nextBatch
  inline def emitRest(db: DBuf): (Array[Char], Array[Int], Array[Int], Int, Array[Int]) =
    db.emitRest
}
