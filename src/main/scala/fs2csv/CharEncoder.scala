package fs2csv

trait CharEncoder[A]:
  def encode(a: A, sb: java.lang.StringBuilder): Unit

object CharEncoder:

  inline def apply[A](using e: CharEncoder[A]): CharEncoder[A] = e

  given CharEncoder[String]  = (a, sb) => { sb.append(a); () }
  given CharEncoder[Int]     = (a, sb) => { sb.append(a); () }
  given CharEncoder[Long]    = (a, sb) => { sb.append(a); () }
  given CharEncoder[Short]   = (a, sb) => { sb.append(a.toInt); () }
  given CharEncoder[Byte]    = (a, sb) => { sb.append(a.toInt); () }
  given CharEncoder[Double]  = (a, sb) => { sb.append(a); () }
  given CharEncoder[Float]   = (a, sb) => { sb.append(a); () }
  given CharEncoder[Boolean] = (a, sb) => { sb.append(a); () }
  given CharEncoder[Char]    = (a, sb) => { sb.append(a); () }

  given CharEncoder[BigInt]     = (a, sb) => { sb.append(a.toString); () }
  given CharEncoder[BigDecimal] = (a, sb) => { sb.append(a.toString); () }

  given option[A](using e: CharEncoder[A]): CharEncoder[Option[A]] =
    (a, sb) => a match {
      case Some(v) => e.encode(v, sb)
      case None    => ()
    }
