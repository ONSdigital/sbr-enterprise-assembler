package converter



import global.Configured
import org.apache.spark.sql.Row

import scala.util.{Random, Try}

/**
* Schema:
* index | fields
* -------------------
*  0 -   BusinessName: string (nullable = true)
*  1 -   CompanyNo: string (nullable = true)
*  2 -   EmploymentBands: string (nullable = true)
*  3 -   IndustryCode: string (nullable = true)
*  4 -   LegalStatus: string (nullable = true)
*  5 -   PayeRefs: array (nullable = true)
*  6 -   PostCode: string (nullable = true)
*  7 -   TradingStatus: string (nullable = true)
*  8 -   Turnover: string (nullable = true)
*  9 -   UPRN: long (nullable = true)
*  10 -  VatRefs: array (nullable = true)
*  11 -  id: long (nullable = true)
  */
trait WithConversionHelper {

  val BusinessName = 0
  val CompanyNo = 1
  val EmploymentBands = 2
  val IndustryCode = 3
  val LegalStatus = 4
  val PayeRefs = 5
  val PostCode = 6
  val TradingStatus = 7
  val Turnover = 8
  val UPRN = 9
  val VatRefs = 10
  val ID = 11
/*
* Rules:
* fields needed for crating ENTERPRISE UNIT:
* 1. ID(UBRN) - NOT NULL
* ## At least one of the below must be present
* 2. PayeRefs  - NULLABLE
* 3. VatRefs - NULLABLE
* 4. CompanyNo - NULLABLE
* */

  import Configured._

  val period = "201802"

  //def printRow(r:Row) =  (0 to 11).foreach(v => println(s"index: $v, name: ${r.schema.fields(v).name}, value: ${Try {r.get(v).toString} getOrElse "NULL"}"))

  def index(r:Row, fieldName:String) = r.schema.fieldIndex(fieldName)

  def getValue[T](row:Row, fieldName:String) = Try{row.getAs[T](index(row,fieldName))}

  def rowToEnt(row:Row): Seq[(String, RowObject)] = {
    //printRow(row)
    val ubrn = row.getAs[Long](index(row,"id"))
    val ern = generateErn//(ubrn.toString)
    val keyStr = generateKey(ern,"ENT")
    createRecord(keyStr,s"C:$ubrn","legalunit")+:rowToLegalUnit(row,ern)
  }



  def rowToLegalUnit(row:Row, ern:String):Seq[(String, RowObject)] = {

    val ubrn: String = row.getAs[Long](index(row,"id")).toString
    val luKey = generateKey(ubrn,"LEU")
    createRecord(luKey,s"P:$ern","enterprise") +: (getCh(row,luKey,ubrn) ++ getVats(row,luKey,ubrn) ++ getPayes(row,luKey,ubrn))
  }

  def getCh(row:Row, luKey:String, ubrn:String):Seq[(String, RowObject)] = getValue[String](row,"CompanyNo").map(companyNo =>
                      if(companyNo.trim.isEmpty) Seq[(String, RowObject)]() else {
                                                          Seq(
                                                            createRecord(luKey,s"C:$companyNo","ch"),
                                                            createRecord(generateKey(companyNo,"CH"),s"P:$ubrn","legalunit")
                                                          )}).getOrElse(Seq[(String, RowObject)]())


  def getVats(row:Row, luKey:String, ubrn:String):Seq[(String, RowObject)] = {
    import scala.collection.JavaConversions._

    getValue[java.util.List[Long]](row,"VatRefs").map(_.toSeq.flatMap(vat => Seq(
                            createRecord(luKey,s"C:$vat","vat"),
                            createRecord(generateKey(vat.toString,"VAT"),s"P:${ubrn.toString}","legalunit")
                         ))).getOrElse {Seq[(String, RowObject)]()}}



  def getPayes(row:Row, luKey:String, ubrn:String):Seq[(String, RowObject)] = {
    import scala.collection.JavaConversions._

    getValue[java.util.List[String]](row,"PayeRefs").map(_.toSeq.flatMap(paye => Seq(
                            createRecord(luKey,s"C:${paye}","paye"),
                            createRecord(generateKey(paye,"PAYE"),s"P:$ubrn","legalunit")
                         ))).getOrElse {Seq[(String, RowObject)]()}}



  private def createRecord(key:String,column:String, value:String) = {
    (key -> RowObject(key,HBASE_ENTERPRISE_COLUMN_FAMILY,column,value) )
  }

  def generateErn(ubrn:String) = s"ENT$ubrn"
  def generateErn = Random.nextInt(9999999).toString //to keep with same format as ubrn
  def generateKey(id:String, suffix:String) = s"$period~$id~$suffix"


}
