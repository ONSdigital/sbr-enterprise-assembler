package dao

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

object DaoUtils {

  implicit class DataFrameExtensions(df: DataFrame) {

    def castAllToString: DataFrame = df.schema.fields.foldLeft(df)((dataFrame, field) => field.dataType match {
      case ArrayType(LongType, nullability) => dataFrame.withColumn(field.name, df.col(field.name).cast(ArrayType(StringType, nullability)))
      case ArrayType(IntegerType, nullability) => dataFrame.withColumn(field.name, df.col(field.name).cast(ArrayType(StringType, nullability)))
      case ArrayType(StringType, nullability) => dataFrame
      case _ => dataFrame.withColumn(field.name, df.col(field.name).cast(StringType))
    }
    )

  }

  implicit class SqlRowExtensions(val row: Row) {
    def getStringWithNull(field: String): Option[String] = {
      val v = getValue[String](field)
      if (v.isDefined && v == null) None
      else v
    }

    /**
      * returns option of value
      * Returns None if field is present but value is null or if field is not present
      * returns Some(VALUE_OF_THE_FIELD) otherwise
      **/
    def getOption[T](field: String): Option[T] = {
      if (row.isNull(field)) None
      else Option[T](row.getAs[T](field))
    }

    /**
      * Returns None if:
      * 1. row does not contain field with given name
      * 2. value is null
      * 3. value's data type is not String
      * returns Some(...) of value
      * otherwise
      **/
    def getStringOption(name: String): Option[String] = {
      getOption[String](name)
    }

    def getValueOrEmptyStr(fieldName: String): String = getStringValueOrDefault(fieldName, "")

    def getStringValueOrDefault(fieldName: String, default: String): String = getStringOption(fieldName).getOrElse(default)

    def getValueOrNull(fieldName: String): String = getStringOption(fieldName).orNull

    def getString(field: String): Option[String] = getValue[String](field)

    def getLong(field: String): Option[Long] = getValue[Long](field)

    def getInteger(field: String): Option[Int] = getValue[Int](field)

    def getStringSeq(field: String): Option[Seq[String]] = getSeq(field, Some((s: String) => s.trim.nonEmpty))

    def getLongSeq(field: String): Option[Seq[Long]] = getSeq[Long](field)

    def getSeq[T](fieldName: String, eval: Option[T => Boolean] = None): Option[Seq[T]] = if (isNull(fieldName)) None else Some(row.getSeq[T](row.fieldIndex(fieldName)).filter(v => v != null && eval.map(_ (v)).getOrElse(true)))

    def isNull(field: String): Boolean = {
      try {
        row.isNullAt(row.fieldIndex(field)) //if field exists and the value is null
      } catch {
        case iae: IllegalArgumentException => true //if field does not exist
        case e: Throwable => {
          println(s"field: ${if (field == null) "null" else field.toString}")
          throw e
        }
      }
    }

    def getCalcValue(fieldName: String): Option[String] = {
      val v = isNull(fieldName)
      if (v) Some("") else Some(row.getAs(fieldName).toString)
    }

    def getValue[T](fieldName: String, eval: Option[T => Boolean] = None): Option[T] = {
      if (isNull(fieldName)) {
        None
      } else {

        val v = row.getAs[T](fieldName)
        if (v.isInstanceOf[String] && v.asInstanceOf[String].trim.isEmpty) None
        else if (v == null) None
        else eval match {
          case Some(f) => if (f(v)) Some(v) else None
          case None => Some(v)
        }
      }
    }
  }

}
