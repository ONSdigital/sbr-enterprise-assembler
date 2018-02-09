package converter



import global.ApplicationContext.config
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
*  6 -   element: string (containsNull = true)
*  7 -   PostCode: string (nullable = true)
*  8 -   TradingStatus: string (nullable = true)
*  9 -   Turnover: string (nullable = true)
*  10 -  UPRN: long (nullable = true)
*  11 -   VatRefs: array (nullable = true)
*  12 -   element: long (containsNull = true)
*  13 -   id: long (nullable = true)
  */
trait WithConversionHelper {

  val period = "201802"
  val idKey = "id"
  val colFamily = config.getString("hbase.local.table.column.family")

  def rowToEnt(row:Row): Seq[(String, RowObject)] = {
    val ubrn = row.getAs[Long](idKey)
    val ern = generateErn//(ubrn.toString)
    val keyStr = generateKey(ern,"ENT")
    createRecord(keyStr,s"C:$ubrn","legalunit")+:rowToLegalUnit(row,ern)
  }

  def rowToLegalUnit(r:Row, ern:String):Seq[(String, RowObject)] = {

    val ubrn: String = r.getAs[Long](idKey).toString
    val luKey = generateKey(ubrn,"LEU")
    val companyNo = r.getAs[String]("CompanyNo")
    createRecord(luKey,s"P:$ern","enterprise") +: (getCh(r,luKey,ubrn) ++ getVats(r,luKey,ubrn) ++ getPayes(r,luKey,ubrn))
  }

  def getCh(r:Row, luKey:String, ubrn:String):Seq[(String, RowObject)] = {
    val companyNo = r.getAs[String]("CompanyNo")

    if(companyNo.trim.isEmpty) Seq[(String, RowObject)]() else {

      Seq(
        createRecord(luKey,s"C:$companyNo","ch"),
        createRecord(generateKey(companyNo,"CH"),s"P:$ubrn","legalunit")
      )}}

  def getVats(r:Row,luKey:String, ubrn:String):Seq[(String, RowObject)] = {
    import scala.collection.JavaConversions._

    Try{r.getList[Long](10)}.map(_.toSeq.flatMap(vat => Seq(
                            createRecord(luKey,s"C:$vat","vat"),
                            createRecord(generateKey(vat.toString,"VAT"),s"P:${ubrn.toString}","legalunit")
                         ))).getOrElse {Seq[(String, RowObject)]()}}



  def getPayes(r:Row,luKey:String, ubrn:String):Seq[(String, RowObject)] = {
    import scala.collection.JavaConversions._

    Try{r.getList[String](5)}.map(_.toSeq.flatMap(paye => Seq(
                            createRecord(luKey,s"C:${paye}","paye"),
                            createRecord(generateKey(paye,"PAYE"),s"P:$ubrn","legalunit")
                         ))).getOrElse {Seq[(String, RowObject)]()}}



  private def createRecord(key:String,column:String, value:String) = {
    (key -> RowObject(key,colFamily,column,value) )
  }

  def generateErn(ubrn:String) = s"ENT$ubrn"
  def generateErn = Random.nextInt(9999999).toString //to keep with same format as ubrn
  def generateKey(id:String, suffix:String) = s"$period~$id~$suffix"


}
