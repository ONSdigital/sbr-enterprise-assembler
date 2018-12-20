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

  val biWithErnSchema = new StructType()
    .add(StructField("ern", StringType,true))
    .add(StructField("ubrn", StringType,false))
    .add(StructField("name", StringType,true))
    .add(StructField("industry_code", StringType,true))
    .add(StructField("legal_status", StringType,true))
    .add(StructField("address1", StringType,true))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,true))
    .add(StructField("trading_status", StringType,true))
    .add(StructField("turnover", StringType,true))
    .add(StructField("uprn", StringType,true))
    .add(StructField("crn", StringType,true))
    .add(StructField("payerefs", ArrayType(StringType,true),true))
    .add(StructField("vatrefs", ArrayType(StringType,true),true))

  val ruRowSchema = new StructType()
    .add(StructField("rurn", StringType,false))
    .add(StructField("ern", StringType,false))
    .add(StructField("name", StringType,false))
    .add(StructField("entref", StringType,true))
    .add(StructField("ruref", StringType,true))
    .add(StructField("trading_style", StringType,true))
    .add(StructField("legal_status", StringType,false))
    .add(StructField("address1", StringType,false))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,false))
    .add(StructField("sic07", StringType,false))
    .add(StructField("employees", StringType,false))
    .add(StructField("turnover", StringType,false))
    .add(StructField("prn", StringType,false))
    .add(StructField("region", StringType,false))
    .add(StructField("employment", StringType,false))


  val leuRowSchema = new StructType()
    .add(StructField("ubrn", StringType,false))
    .add(StructField("ern", StringType,false))
    .add(StructField("prn", StringType,false))
    .add(StructField("crn", StringType,true))
    .add(StructField("name", StringType,false))
    .add(StructField("trading_style", StringType,true))
    .add(StructField("address1", StringType,false))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,false))
    .add(StructField("sic07", StringType,false))
    .add(StructField("paye_jobs", StringType,true))
    .add(StructField("turnover", StringType,true))
    .add(StructField("legal_status", StringType,false))
    .add(StructField("trading_status", StringType,true))
    .add(StructField("birth_date", StringType,false))
    .add(StructField("death_date", StringType,true))
    .add(StructField("death_code", StringType,true))
    .add(StructField("uprn", StringType,true))

  val preCalculateDfSchema = new StructType()
    .add(StructField("ern", StringType,true))
    .add(StructField("id", StringType,true))
    .add(StructField("CompanyNo", StringType,true))
    .add(StructField("PayeRefs", ArrayType(StringType,true),true))
    .add(StructField("VatRefs", ArrayType(StringType,true),true))


  val existingLuBiRowSchema = new StructType()
    .add(StructField("id", StringType,true))
    .add(StructField("ern", StringType,true))

  val linksLeuRowSchema = new StructType()
    .add(StructField("ubrn", StringType,false))
    .add(StructField("ern", StringType,true))
    .add(StructField("crn", StringType,true))
    .add(StructField("payerefs", ArrayType(StringType,true),true))
    .add(StructField("vatrefs", ArrayType(StringType,true),true))

  val louRowSchema = new StructType()
    .add(StructField("lurn", StringType,false))
    .add(StructField("luref", StringType,true))
    .add(StructField("ern", StringType,false))
    .add(StructField("prn", StringType,false))
    .add(StructField("rurn", StringType,false))
    .add(StructField("ruref", StringType,true))
    .add(StructField("name", StringType,false))
    .add(StructField("entref", StringType,true))
    .add(StructField("trading_style", StringType,true))
    .add(StructField("address1", StringType,false))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,false))
    .add(StructField("region", StringType,false))
    .add(StructField("sic07", StringType,false))
    .add(StructField("employees", StringType,false))
    .add(StructField("employment", StringType,false))


  val ernToEmployeesSchema = new StructType()
    .add(StructField("ern", StringType,true))
    .add(StructField("PayeRefs", ArrayType(StringType,true),true))
    .add(StructField("VatRefs", ArrayType(StringType,true),true))


  val louIdsSchema = new StructType()
    .add(StructField("ern", StringType,true))
    .add(StructField("lurn", StringType,true))


  val entRowSchema = new StructType()
    .add(StructField("ern", StringType,false))
    .add(StructField("prn", StringType,false))
    .add(StructField("entref", StringType,true))
    .add(StructField("name", StringType,true))
    .add(StructField("trading_style", StringType,true))
    .add(StructField("address1", StringType,false))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,false))
    .add(StructField("sic07", StringType,false))
    .add(StructField("legal_status", StringType,false))
    .add(StructField("working_props", StringType,false))


  val entRowWithEmplDataSchema = new StructType()
    .add(StructField("ern", StringType,false))
    .add(StructField("prn", StringType,false))
    .add(StructField("idbrref", StringType,true))
    .add(StructField("name", StringType,true))
    .add(StructField("trading_style", StringType,true))
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

  val completeEntSchema = new StructType()
    .add(StructField("ern", StringType,false))
    .add(StructField("prn", StringType,false))
    .add(StructField("entref", StringType,true))
    .add(StructField("name", StringType,false))
    .add(StructField("trading_style", StringType,true))
    .add(StructField("address1", StringType,false))
    .add(StructField("address2", StringType,true))
    .add(StructField("address3", StringType,true))
    .add(StructField("address4", StringType,true))
    .add(StructField("address5", StringType,true))
    .add(StructField("postcode", StringType,false))
    .add(StructField("region", StringType,false))
    .add(StructField("sic07", StringType,false))
    .add(StructField("legal_status", StringType,false))
    .add(StructField("paye_empees", StringType,true))
    .add(StructField("paye_jobs", StringType,true))
    .add(StructField("cntd_turnover", StringType,true))
    .add(StructField("app_turnover", StringType,true))
    .add(StructField("std_turnover", StringType,true))
    .add(StructField("grp_turnover", StringType,true))
    .add(StructField("ent_turnover", StringType,true))
    .add(StructField("working_props", StringType,false))
    .add(StructField("employment", StringType,false))


val calculationsSchema = new StructType()
  .add(StructField("ern", StringType,true))
  .add(StructField("paye_empees", StringType,true))
  .add(StructField("paye_jobs", StringType,true))
  .add(StructField("app_turnover", StringType,true))
  .add(StructField("ent_turnover", StringType,true))
  .add(StructField("cntd_turnover", StringType,true))
  .add(StructField("std_turnover", StringType,true))
  .add(StructField("grp_turnover", StringType,true))

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
/**
  * returns option of value
  * Retuns None if field is present but value is null or if field is not present
  * returns Some(VALUE_OF_THE_FIELD) otherwise
  * */
    def getOption[T](field:String)= {
      if(row.isNull(field)) None
      else Option[T](row.getAs[T](field))
    }

    /**
      * Returns None if:
      * 1. row does not contain field with given name
      * 2. value is null
      * 3. value's data type is not String
      * returns Some(...) of value
      * otherwise
      * */
    def getStringOption(name:String) = {
      getOption[String](name)
    }

    def getValueOrEmptyStr(fieldName:String) = getStringValueOrDefault(fieldName,"")

    def getStringValueOrDefault(fieldName:String,default:String) = getStringOption(fieldName).getOrElse(default)

    def getValueOrNull(fieldName:String) = getStringOption(fieldName).getOrElse(null)

    def getString(field:String): Option[String] = getValue[String](field)

    def getLong(field:String): Option[Long] = getValue[Long](field)

    def getInteger(field:String): Option[Int] = getValue[Int](field)

    def getStringSeq(field:String): Option[Seq[String]] = getSeq(field,Some((s:String) => s.trim.nonEmpty))

    def getLongSeq(field:String): Option[Seq[Long]] = getSeq[Long](field)

    def getSeq[T](fieldName:String, eval:Option[T => Boolean] = None): Option[Seq[T]] = if(isNull(fieldName)) None else Some(row.getSeq[T](row.fieldIndex(fieldName)).filter(v => v!=null && eval.map(_(v)).getOrElse(true)))

    def isNull(field:String) = try {
          row.isNullAt(row.fieldIndex(field))  //if field exists and the value is null
        }catch {
          case iae:IllegalArgumentException => true  //if field does not exist
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
