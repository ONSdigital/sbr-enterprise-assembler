package converter



import global.Configs
import org.apache.spark.sql.Row

import scala.util.{Random, Success, Try}

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

  import Configs._

  val period = "201802"

  //def printRow(r:Row) =  (0 to 11).foreach(v => println(s"index: $v, name: ${r.schema.fields(v).name}, value: ${Try {r.get(v).toString} getOrElse "NULL"}"))
  def getValue[T](row:Row, fieldName:String) = Try{row.getAs[T](fieldName)}

  def toRecords(row:Row): Tables = {
    val ern = generateErn
    Tables(rowToEnterprise(row,ern),rowToLinks(row,ern))
  }

  def rowToEnterprise(row:Row,ern:String): Seq[(String, RowObject)] = {

    val idbr = "9999999999"
    Seq(createEnterpriseRecord(ern,"ern",ern), createEnterpriseRecord(ern,"idbrref","9999999999"))++
                         Seq(
                              getValue[String](row, "BusinessName").map(bn => createEnterpriseRecord(ern,"name",bn)),
                              getValue[String](row, "PostCode").map(pc => createEnterpriseRecord(ern,"postcode",pc)),
                              getValue[String](row, "LegalStatus").map(ls => createEnterpriseRecord(ern,"legalstatus",ls))
                            ).collect{case Success(v) => v}
  }


  def rowToLinks(row:Row,ern:String): Seq[(String, RowObject)] = {
    //printRow(row)
    val ubrn = row.getAs[Long]("id")
    val keyStr = generateKey(ern,"ENT")
    createLinksRecord(keyStr,s"C:$ubrn","legalunit")+:rowToLegalUnitLinks(row,ern)
  }



  def rowToLegalUnitLinks(row:Row, ern:String):Seq[(String, RowObject)] = {

    val ubrn: String = row.getAs[Long](("id")).toString
    val luKey = generateKey(ubrn,"LEU")
    createLinksRecord(luKey,s"P:$ern","enterprise") +: (rowToCHLinks(row,luKey,ubrn) ++ rowToVatRefsLinks(row,luKey,ubrn) ++ rowToPayeRefLinks(row,luKey,ubrn))
  }

  def rowToCHLinks(row:Row, luKey:String, ubrn:String):Seq[(String, RowObject)] = getValue[String](row,"CompanyNo").map(companyNo =>
                      if(companyNo.trim.isEmpty) Seq[(String, RowObject)]() else {
                                                          Seq(
                                                            createLinksRecord(luKey,s"C:$companyNo","ch"),
                                                            createLinksRecord(generateKey(companyNo,"CH"),s"P:$ubrn","legalunit")
                                                          )}).getOrElse(Seq[(String, RowObject)]())


  def rowToVatRefsLinks(row:Row, luKey:String, ubrn:String):Seq[(String, RowObject)] = {
    import scala.collection.JavaConversions._

    getValue[java.util.List[Long]](row,"VatRefs").map(_.toSeq.flatMap(vat => Seq(
                            createLinksRecord(luKey,s"C:$vat","vat"),
                            createLinksRecord(generateKey(vat.toString,"VAT"),s"P:${ubrn.toString}","legalunit")
                         ))).getOrElse {Seq[(String, RowObject)]()}}



  def rowToPayeRefLinks(row:Row, luKey:String, ubrn:String):Seq[(String, RowObject)] = {
    import scala.collection.JavaConversions._

    getValue[java.util.List[String]](row,"PayeRefs").map(_.toSeq.flatMap(paye => Seq(
                            createLinksRecord(luKey,s"C:${paye}","paye"),
                            createLinksRecord(generateKey(paye,"PAYE"),s"P:$ubrn","legalunit")
                         ))).getOrElse {Seq[(String, RowObject)]()}}



  private def createLinksRecord(key:String,column:String, value:String) = {
    createRecord(key,HBASE_LINKS_COLUMN_FAMILY,column,value)
  }
  private def createEnterpriseRecord(ern:String,column:String, value:String) = {
    val key= s"${ern.reverse}~$period"
    createRecord(key,HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)
  }

  private def createRecord(key:String,columnFamily:String, column:String, value:String) = (key -> RowObject(key,columnFamily,column,value) )

  def generateErn(ubrn:String) = s"ENT$ubrn"
  def generateErn = Random.nextInt(9999999).toString //to keep with same format as ubrn
  def generateKey(id:String, suffix:String) = s"$period~$id~$suffix"


}
