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

/*  def rowToEnt(r:Row): Seq[(ImmutableBytesWritable, KeyValue)] = {
    val ern = Random.nextInt.toString
    val ubnr = r.getAs[Long](idKey)
    val keyStr = s"$period~${ern}~ENT"
    val luKeyValue = new KeyValue(toBytes(keyStr), toBytes(config.getString("hbase.local.table.column.family")), toBytes(ubnr), toBytes("legalunit") )
    val key =  new ImmutableBytesWritable(toBytes(keyStr))
    (key, luKeyValue)+:rowToLegalUnit(r,ern)
  }*/
def generateErn = s"$period~${Random.nextInt.toString}~ENT"

  def rowToEnt(row:Row): Seq[(String, RowObject)] = {
    val ubnr = row.getAs[Long](idKey)
    val ern = generateErn
    val keyStr = s"$period~${ern}~ENT"
    val luKeyValue = RowObject(keyStr, colFamily, ubnr.toString, "legalunit" )
    //val key =  new ImmutableBytesWritable(toBytes(keyStr))
    (keyStr -> luKeyValue)+:rowToLegalUnit(row,ern)
  }


/*  def rowToLegalUnit(r:Row, ern:String):Seq[(ImmutableBytesWritable, KeyValue)] = {
    import scala.collection.JavaConversions._
    val ubnr: Long = r.getAs[Long](idKey)
    val keyStr = s"$period~${ubnr}~LEU"
    val key =  new ImmutableBytesWritable(toBytes(keyStr))
    val luEntEntry = (key,keyValue(keyStr,ern,"enterprise"))
    val chRefsEntEntry = r.getAs[String]("CompanyNo")//(key, keyValue(ern,"enterprise"))
    val vatRefs = r.getList[Long](10)
    val u:Unit = if(!vatRefs.isEmpty && vatRefs.size>1) {
      println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
      val s = vatRefs//.asScala.toList
      s.foreach(println)
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    }
    if(chRefsEntEntry.trim.isEmpty) Seq(luEntEntry) else {

      Seq(luEntEntry, (key, keyValue(keyStr,chRefsEntEntry,"ch"))):+createCh(chRefsEntEntry,ubnr)//(new ImmutableBytesWritable(strToBytes(s"$period~${chRefsEntEntry}~CH")),keyValue(ubnr,"legalunit") )
    }
  }*/


  def rowToLegalUnit(r:Row, ern:String):Seq[(String, RowObject)] = {
    import scala.collection.JavaConversions._

    val ubnr: String = r.getAs[Long](idKey).toString
    val keyStr = s"$period~${ubnr}~LEU"
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

      Seq(luEntEntry/*, (keyStr -> RowObject(keyStr,colFamily,companyNo,"ch"))*/):+createCh(companyNo,ubnr)//(new ImmutableBytesWritable(strToBytes(s"$period~${chRefsEntEntry}~CH")),keyValue(ubnr,"legalunit") )

  }}



/*  def createCh(coNo:String,ubnr:Long) = {
    val key = s"$period~${coNo}~CH"
    (new ImmutableBytesWritable(toBytes(key)),keyValue(key,ubnr,"legalunit") )
  }*/

  def createCh(coNo:String,ubnr:String) = {
    val key = s"$period~${coNo}~CH"
    (key -> RowObject(key,colFamily,ubnr,"legalunit") )
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
