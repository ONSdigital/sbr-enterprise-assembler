package converter



import global.ApplicationContext.config
import org.apache.spark.sql.Row

import scala.util.Random

/**
  *
  */
trait WithConversionHelper {

  val period = "201802"
  val idKey = "id"
  val colFamily = config.getString("hbase.local.table.column.family")

  def rowToEnt(row:Row): Seq[(String, RowObject)] = {
    val ubrn = row.getAs[Long](idKey)
    val ern = generateErn(ubrn.toString)
    val keyStr = generateKey(ern,"ENT")
    createRecord(keyStr,ubrn.toString,"legalunit")+:rowToLegalUnit(row,ern)
  }

  def rowToLegalUnit(r:Row, ern:String):Seq[(String, RowObject)] = {

    val ubrn: String = r.getAs[Long](idKey).toString
    val luKey = generateKey(ubrn,"LEU")
    val companyNo = r.getAs[String]("CompanyNo")//(key, keyValue(ern,"enterprise"))
/*    val vatRefs = r.getList[Long](10)
    val u:Unit = if(!vatRefs.isEmpty && vatRefs.size>1) {
      println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
      val s = vatRefs//.asScala.toList
      s.foreach(println)
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    }*/
    createRecord(luKey,ern,"enterprise") +: getCh(r,luKey)
  }

  def getCh(r:Row, luKey:String):Seq[(String, RowObject)] = {

    val ubrn: String = r.getAs[Long](idKey).toString
    val companyNo = r.getAs[String]("CompanyNo")

    if(companyNo.trim.isEmpty) Seq[(String, RowObject)]() else {

      Seq(
        createRecord(luKey,companyNo,"ch"),
        createRecord(generateKey(companyNo,"CH"),ubrn,"legalunit")
      )}}

  private def createRecord(key:String,column:String, value:String) = {
    (key -> RowObject(key,colFamily,column,value) )
  }

  def generateErn(ubrn:String) = s"ENT$ubrn"
  def generateKey(id:String, suffix:String) = s"$period~$id~$suffix"


}
