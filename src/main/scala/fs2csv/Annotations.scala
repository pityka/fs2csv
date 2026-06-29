package fs2csv

import scala.quoted.*

private[fs2csv] object Annotations:

  inline def csvNames[A]: Array[String | Null] = ${ csvNamesImpl[A] }

  private def csvNamesImpl[A: Type](using q: Quotes): Expr[Array[String | Null]] = {
    import q.reflect.*
    val classSym   = TypeRepr.of[A].typeSymbol
    val csvNameTpe = TypeRepr.of[fs2csv.csvName]

    val perFieldName: List[Expr[String | Null]] = classSym.caseFields.map { fld =>
      val annotated: Option[String] = fld.annotations.collectFirst {
        case Apply(Select(New(tpt), _), List(Literal(StringConstant(s))))
            if tpt.tpe =:= csvNameTpe =>
          s
      }
      annotated match {
        case Some(s) =>
          if (s.isEmpty)
            report.errorAndAbort(
              s"@csvName on ${classSym.name}.${fld.name}: name must not be empty"
            )
          '{ ${Expr(s)}: String | Null }
        case None =>
          '{ null: String | Null }
      }
    }

    '{ Array[String | Null](${ Varargs(perFieldName) }*) }
  }
