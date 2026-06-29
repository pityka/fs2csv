package fs2csv

object Naming:

  val identity: String => String = s => s

  val snakeCase: String => String = name => splitCamelCaseTokens(name).mkString("_").toLowerCase

  val kebabCase: String => String = name => splitCamelCaseTokens(name).mkString("-").toLowerCase

  val pascalCase: String => String = name =>
    if (name.isEmpty) name else s"${name.head.toUpper}${name.tail}"

  val upperSnakeCase: String => String = name => splitCamelCaseTokens(name).mkString("_").toUpperCase

  val lowerCase: String => String = _.toLowerCase

  val upperCase: String => String = _.toUpperCase

  def fromMap(fieldNameToHeader: Map[String, String]): String => String =
    name => fieldNameToHeader.getOrElse(name, name)

  def fromMap(
      fieldNameToHeader: Map[String, String],
      fallback: String => String
  ): String => String =
    name => fieldNameToHeader.getOrElse(name, fallback(name))

  private def splitCamelCaseTokens(name: String): List[String] = {
    if (name.isEmpty) return List("")
    val tokens = scala.collection.mutable.ListBuffer.empty[String]
    var tokenStart = 0
    var i = 1
    while (i < name.length) {
      val cur  = name.charAt(i)
      val prev = name.charAt(i - 1)
      val atTokenBoundary =
        (cur.isUpper && prev.isLower) ||
          (cur.isLetter && !prev.isLetter)
      if (atTokenBoundary) {
        tokens += name.substring(tokenStart, i)
        tokenStart = i
      }
      i += 1
    }
    tokens += name.substring(tokenStart)
    tokens.toList
  }
