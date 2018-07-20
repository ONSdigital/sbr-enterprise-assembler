package dao.hbase.converter

import global.AppParams
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row}
import spark.extensions.sql._

import scala.util.{Random, Try}

trait UnitRowConverter {

/**
  * Created from new BI record
  * */
  def biRowToEnt(biRow:Row): GenericRowWithSchema = {
    //globals:
    val ern = generateErn(biRow,null)
    val lurn = generateLurn(biRow,null)
    //ents attrs:
      val entRef = "9999999999"//biRow.getString("entRef")
      val name = Try{biRow.row.getAs[String]("BusinessName")}.getOrElse("")
      val postCode = Try{biRow.row.getAs[String]("PostCode")}.getOrElse("")
      val sic07 = Try{biRow.row.getAs[String]("IndustryCode")}.getOrElse("")
      val address1 = Try{biRow.row.getAs[String]("address1")}.getOrElse("")
      val legalStatus = Try{biRow.row.getAs[String]("LegalStatus")}.getOrElse(null)
//link record attrs:

      val ubrn = biRow.getAs[String]("id")
      val ch = Try{biRow.getAs[String]("CompanyNo")}.getOrElse(null)
      val payeRefs: Seq[String] = Try{biRow.getAs[Seq[String]]("PayeRefs")}.getOrElse(null)
      val vatRefs = Try{biRow.getAs[Seq[String]]("VatRefs")}.getOrElse(null)


    new GenericRowWithSchema(Array(
                                            ern,entRef,name,null,address1,null, null, null,null,postCode,sic07,legalStatus,
                                            ubrn, lurn, ch, payeRefs,vatRefs
                                          ),entWithLinksRowSchema)

  }


  def generateErn(row:Row, appParams:AppParams) = generateUniqueKey
  def generateLurn(row:Row, appParams:AppParams) = generateUniqueKey
  def generateLurnFromEnt(row:Row, appParams:AppParams) = generateUniqueKey

  def generateUniqueKey = Random.alphanumeric.take(18).mkString

}
