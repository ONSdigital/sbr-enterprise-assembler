package spark.extensions

import org.apache.spark.sql.Row

/**
  *
  */
package object sql {

  implicit class SqlRowExtensions(val row:Row) {

    def getString(field:String): Option[String] = getValue[String](field)

    def getLong(field:String): Option[Long] = getValue[Long](field)

    def getStringSeq(field:String): Option[Seq[String]] = getSeq(field,Some((s:String) => s.trim.nonEmpty))

    def getLongSeq(field:String): Option[Seq[Long]] = getSeq[Long](field)

    def getSeq[T](fieldName:String, eval:Option[T => Boolean] = None): Option[Seq[T]] = if(isNull(fieldName)) None else Some(row.getSeq[T](row.fieldIndex(fieldName)).filter(v => v!=null && eval.map(_(v)).getOrElse(true)))

    def isNull(field:String) = row.isNullAt(row.fieldIndex(field))

    def getValue[T](
                   fieldName:String,
                   eval:Option[T => Boolean] = None
                 ): Option[T] = if(isNull(fieldName)) None else {

      val v = row.getAs[T](fieldName)
      if (v.isInstanceOf[String] && v.asInstanceOf[String].trim.isEmpty) None
      else eval match{
        case Some(f) => if(f(v)) Some(v) else None
        case None  => Some(v)
      }}

   }
}
