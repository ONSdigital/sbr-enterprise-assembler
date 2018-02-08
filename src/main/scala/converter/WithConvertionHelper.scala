package converter



import converter.DataConverter.period
import global.ApplicationContext.config
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row

import scala.util.Random

/**
  *
  */
trait WithConversionHelper {

  val period = "201802"
  val idKey = "id"
  val colFamily = config.getString("hbase.local.table.column.family")

  def generateErn = Random.nextInt.toString
  def generateKey(id:String, suffix:String) = s"$period~$id~$suffix"

  def rowToEnt(row:Row): Seq[(String, RowObject)] = {
    val ubnr = row.getAs[Long](idKey)
    val ern = generateErn
    val keyStr = generateKey(ern,"ENT")
    val luKeyValue = RowObject(keyStr, colFamily, ubnr.toString, "legalunit" )
    (keyStr -> luKeyValue)+:rowToLegalUnit(row,ern)
  }



  def rowToLegalUnit(r:Row, ern:String):Seq[(String, RowObject)] = {

    val ubrn: String = r.getAs[Long](idKey).toString
    val keyStr = generateKey(ubrn,"LEU")
    val luEntEntry = (keyStr -> RowObject(keyStr,colFamily,ern,"enterprise"))
    val companyNo = r.getAs[String]("CompanyNo")//(key, keyValue(ern,"enterprise"))
/*    val vatRefs = r.getList[Long](10)
    val u:Unit = if(!vatRefs.isEmpty && vatRefs.size>1) {
      println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
      val s = vatRefs//.asScala.toList
      s.foreach(println)
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    }*/
   if(companyNo.trim.isEmpty) Seq(luEntEntry) else {

      Seq((keyStr -> RowObject(keyStr,colFamily,ern,"enterprise")),
          (keyStr -> RowObject(keyStr,colFamily,companyNo,"ch"))):+createCh(companyNo,ubrn)

  }}

  def createCh(coNo:String,ubrn:String) = {
    val key = generateKey(coNo,"CH")
    (key -> RowObject(key,colFamily,ubrn,"legalunit") )
  }

  def toBytes(s:String) = try{
    s.getBytes()
  }catch{
    case e:Throwable => {
      throw new Exception(e)
    }
    case _ => throw new Exception(s"cannot do bytes from this string: $s")
  }
  def toBytes(l:Long) = try{
    Bytes.toBytes(l)
  }catch{
    case e:Throwable => {
      throw new Exception(e)
    }
    case _ => throw new Exception(s"cannot do bytes from long: $l")
  }

  private def keyValue(key:String,qualifier:String,value:String):KeyValue = new KeyValue(toBytes(key), toBytes(colFamily), toBytes(qualifier), toBytes(value) )

  private def keyValue(key:String,qualifier:Long,value:String):KeyValue = new KeyValue(toBytes(key), toBytes(colFamily), toBytes(qualifier), toBytes(value) )


}
