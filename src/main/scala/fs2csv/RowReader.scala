package fs2csv

import scala.compiletime.constValueTuple
import scala.compiletime.summonAll
import scala.deriving.Mirror

trait RowReader[A]:
  def arity: Int
  def fieldLabels: Array[String]
  def headerName(idx: Int, transform: String => String): String
  def setField(idx: Int, chars: Array[Char], from: Int, until: Int): Unit
  def build(): A
  def reset(): Unit

object RowReader:

  inline def apply[A](using r: RowReader[A]): RowReader[A] = r

  inline given derived[A <: Product](using m: Mirror.ProductOf[A]): RowReader[A] = {
    val decodersRaw: Array[Object] =
      summonAll[Tuple.Map[m.MirroredElemTypes, CharDecoder]].toArray
    val labelsRaw: Array[Object] =
      constValueTuple[m.MirroredElemLabels].toArray
    val fieldLabels: Array[String] = {
      val a = new Array[String](labelsRaw.length)
      var i = 0
      while (i < a.length) { a(i) = labelsRaw(i).asInstanceOf[String]; i += 1 }
      a
    }
    val csvNameOverrides: Array[String | Null] = Annotations.csvNames[A]
    new MirrorDerivedRowReader[A](decodersRaw, fieldLabels, csvNameOverrides, m)
  }

  private[fs2csv] final class MirrorDerivedRowReader[A](
      private val decoders:         Array[Object],
      private val labels:           Array[String],
      private val csvNameOverrides: Array[String | Null],
      private val mirror:           Mirror.ProductOf[A]
  ) extends RowReader[A]:
    private val fieldCount        = decoders.length
    private val rowBuffer         = new Array[Any](fieldCount)
    private val rowAsProduct: Product = new BackingArrayProduct(rowBuffer)

    val arity: Int                 = fieldCount
    def fieldLabels: Array[String] = labels

    def headerName(idx: Int, transform: String => String): String = {
      val explicit = csvNameOverrides(idx)
      if (explicit eq null) transform(labels(idx)) else explicit
    }

    def setField(idx: Int, chars: Array[Char], from: Int, until: Int): Unit =
      rowBuffer(idx) = decoders(idx).asInstanceOf[CharDecoder[Any]].decode(chars, from, until)

    def build(): A = mirror.fromProduct(rowAsProduct)

    def reset(): Unit = {
      var i = 0
      while (i < fieldCount) { rowBuffer(i) = null; i += 1 }
    }

  private[fs2csv] final class BackingArrayProduct(arr: Array[Any]) extends Product:
    def productArity: Int           = arr.length
    def productElement(n: Int): Any = arr(n)
    def canEqual(that: Any): Boolean = that.isInstanceOf[BackingArrayProduct]
