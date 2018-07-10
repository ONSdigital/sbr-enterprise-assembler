package spark.extensions

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
  *
  */
package object sql {

  val parquetRowSchema = new StructType()
    .add(StructField("BusinessName", StringType,true))
    .add(StructField("CompanyNo", StringType,true))
    .add(StructField("EmploymentBands", StringType,true))
    .add(StructField("IndustryCode", StringType,true))
    .add(StructField("LegalStatus", StringType,true))
    .add(StructField("PayeRefs", ArrayType(StringType,true),true))
    .add(StructField("PostCode", StringType,true))
    .add(StructField("TradingStatus", StringType,true))
    .add(StructField("Turnover", StringType,true))
    .add(StructField("UPRN", StringType,true))
    .add(StructField("VatRefs", ArrayType(StringType,true),true))
    .add(StructField("id", StringType,false))

  val luRowSchema = new StructType()
    .add(StructField("ubrn", StringType,false))
    .add(StructField("ern", StringType,true))
    .add(StructField("CompanyNo", StringType,true))
    .add(StructField("PayeRefs", ArrayType(StringType,true),true))
    .add(StructField("VatRefs", ArrayType(StringType,true),true))

  val louRowSchema = new StructType()
    .add(StructField("lurn", StringType,false))
    .add(StructField("luref", StringType,true))
    .add(StructField("ern", StringType,true))
    .add(StructField("entref", StringType,true))
    .add(StructField("name", StringType,false))
    .add(StructField("tradingstyle", StringType,true))
    .add(StructField("address1", StringType,false))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,false))
    .add(StructField("sic07", StringType,false))
    .add(StructField("employees", StringType,false))

  val ernToEmployeesSchema = new StructType()
    .add(StructField("ern", StringType,true))
    .add(StructField("PayeRefs", ArrayType(StringType,true),true))
    .add(StructField("VatRefs", ArrayType(StringType,true),true))


  val louIdsSchema = new StructType()
    .add(StructField("ern", StringType,true))
    .add(StructField("lurn", StringType,true))


  val entRowSchema = new StructType()
    .add(StructField("ern", StringType,false))
    .add(StructField("entref", StringType,true))
    .add(StructField("name", StringType,true))
    .add(StructField("trading_style", StringType,true))
    .add(StructField("address1", StringType,false))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,true))
    .add(StructField("sic07", StringType,true))
    .add(StructField("legal_status", StringType,true))


  val entRowWithEmplDataSchema = new StructType()
    .add(StructField("ern", StringType,false))
    .add(StructField("idbrref", StringType,true))
    .add(StructField("name", StringType,true))
    .add(StructField("tradingstyle", StringType,true))
    .add(StructField("address1", StringType,true))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,true))
    .add(StructField("sic07", StringType,true))
    .add(StructField("legalstatus", StringType,true))
    .add(StructField("paye_employees", StringType,true))
    .add(StructField("paye_jobs", StringType,true))

  implicit class DataFrameExtensions(df:DataFrame){

    def castAllToString() =  df.schema.fields.foldLeft(df)((dataFrame, field) => field.dataType match{
        case ArrayType(LongType, nullability) =>  dataFrame.withColumn(field.name,df.col(field.name).cast((ArrayType(StringType,nullability))))
        case ArrayType(IntegerType, nullability) =>  dataFrame.withColumn(field.name,df.col(field.name).cast((ArrayType(StringType,nullability))))
        case ArrayType(StringType, nullability) =>  dataFrame
        case _  => dataFrame.withColumn(field.name,df.col(field.name).cast((StringType)))
      }
    )

  }

  implicit class SqlRowExtensions(val row:Row) {

    def getStringWithNull(field:String): Option[String] = {
      val v = getValue[String](field)
      if (v.isDefined && v==null) None
      else v
    }


    def getString(field:String): Option[String] = getValue[String](field)

    def getLong(field:String): Option[Long] = getValue[Long](field)

    def getInteger(field:String): Option[Int] = getValue[Int](field)

    def getStringSeq(field:String): Option[Seq[String]] = getSeq(field,Some((s:String) => s.trim.nonEmpty))

    def getLongSeq(field:String): Option[Seq[Long]] = getSeq[Long](field)

    def getSeq[T](fieldName:String, eval:Option[T => Boolean] = None): Option[Seq[T]] = if(isNull(fieldName)) None else Some(row.getSeq[T](row.fieldIndex(fieldName)).filter(v => v!=null && eval.map(_(v)).getOrElse(true)))

    def isNull(field:String) = try {
          row.isNullAt(row.fieldIndex(field))
        }catch {
          case iae:IllegalArgumentException => true
          case e: Throwable => {
              println(s"field: ${if (field==null) "null" else field.toString()}")
              throw e
             }
        }

      def getCalcValue(fieldName:String): Option[String] = {
        val v = isNull(fieldName)
        v match{
          case true  => Some("")
          case false => Some(row.getAs(fieldName).toString)
        }}

      def getValue[T](
                       fieldName:String,
                       eval:Option[T => Boolean] = None
                     ): Option[T] = if(isNull(fieldName)) None else {

        val v = row.getAs[T](fieldName)
        if (v.isInstanceOf[String] && v.asInstanceOf[String].trim.isEmpty) None
        else if (v==null) None
        else eval match{
          case Some(f) => if(f(v)) Some(v) else None
          case None  => Some(v)
        }}
    }


}
