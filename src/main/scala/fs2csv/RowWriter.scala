package fs2csv

import scala.compiletime.constValueTuple
import scala.compiletime.summonAll
import scala.deriving.Mirror

trait RowWriter[A] {
  def arity: Int
  def fieldLabels: Array[String]
  def headerName(idx: Int, transform: String => String): String
  def encodeField(a: A, idx: Int, sb: java.lang.StringBuilder): Unit
}

object RowWriter {

  inline def apply[A](using w: RowWriter[A]): RowWriter[A] = w

  inline given derived[A <: Product](using m: Mirror.ProductOf[A]): RowWriter[A] = {
    val encodersRaw: Array[Object] =
      summonAll[Tuple.Map[m.MirroredElemTypes, CharEncoder]].toArray
    val labelsRaw: Array[Object] =
      constValueTuple[m.MirroredElemLabels].toArray
    val fieldLabels: Array[String] = {
      val a = new Array[String](labelsRaw.length)
      var i = 0
      while (i < a.length) { a(i) = labelsRaw(i).asInstanceOf[String]; i += 1 }
      a
    }
    val csvNameOverrides: Array[String | Null] = Annotations.csvNames[A]
    new MirrorDerivedRowWriter[A](encodersRaw, fieldLabels, csvNameOverrides)
  }

  private[fs2csv] final class MirrorDerivedRowWriter[A <: Product](
      private val encoders:         Array[Object],
      private val labels:           Array[String],
      private val csvNameOverrides: Array[String | Null]
  ) extends RowWriter[A] {
    val arity: Int                 = encoders.length
    def fieldLabels: Array[String] = labels

    def headerName(idx: Int, transform: String => String): String = {
      val explicit = csvNameOverrides(idx)
      if (explicit eq null) transform(labels(idx)) else explicit
    }

    def encodeField(a: A, idx: Int, sb: java.lang.StringBuilder): Unit =
      encoders(idx).asInstanceOf[CharEncoder[Any]].encode(a.productElement(idx), sb)
  }
}
