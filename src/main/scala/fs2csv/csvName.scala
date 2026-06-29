package fs2csv

import scala.annotation.{StaticAnnotation, meta}

@meta.field
final class csvName(val name: String) extends StaticAnnotation
