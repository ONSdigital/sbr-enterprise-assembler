package service

import dao.hbase.HBaseDao
import global.{AppParams, Configs}
import model.domain.HFileRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.{RddLogging, SparkSessionManager}
import spark.extensions.sql.parquetRowSchema



case class DataReport(entCount:Long, lusCount:Long, losCount:Long, childlessEntErns:Seq[String], lusOrphans:Seq[(String,(String,String))], losOrphans:Seq[(String,(String,String))])

object InputAnalyser extends RddLogging{

  def getData(appconf:AppParams)(implicit spark: SparkSession):DataReport =  {

    val entRdd: RDD[HFileRow] = HBaseDao.readEnterprisesWithKeyFilter(Configs.conf,appconf, ".*~"+{appconf.TIME_PERIOD}+"$")
    entRdd.cache()
    //printRdd("enterprises",entRdd,"HFileRow")
    val lusRdd: RDD[HFileRow] = HBaseDao.readLinksWithKeyFilter(Configs.conf,appconf, ".*~LEU~"+{appconf.TIME_PERIOD}+"$")
                                                        .collect{ case row if(row.cells.find(_.column == "p_ENT").isDefined) => row }
    //printRdd("LEU",lusRdd,"HFileRow")

    val losRdd: RDD[HFileRow] = HBaseDao.readLouWithKeyFilter(Configs.conf,appconf, ".*~"+{appconf.TIME_PERIOD}+"~.*")
    //printRdd("LOU",losRdd,"HFileRow")

    val entErns = entRdd.map(row => row.key.split("~").head.reverse)
    entErns.cache()

    val luErns: RDD[String] = lusRdd.map(row => row.cells.find(_.column == "p_ENT").get.value)
    val loErns: RDD[String] = losRdd.map(row => row.cells.find(_.column == "ern").get.value)

    val luIntersection: RDD[String] = entErns.intersection(luErns.distinct())
    val loIntersection: RDD[String] = entErns.intersection(loErns.distinct())
    val orphanLus: RDD[(String, (String, String))] = getOrphanLus(lusRdd,luIntersection)
    val orphanLos: RDD[(String, (String, String))] = getOrphanLos(losRdd,loIntersection)
    val childlessEnts = getChildlessEnts(entErns,luErns,loErns)
    val entCount = entRdd.count()

    entRdd.unpersist()

    val res = DataReport(entCount,lusRdd.count(),losRdd.count(),childlessEnts.collect(), orphanLus.collect(),orphanLos.collect())

    spark.stop()
    res
  }

  def getChildlessEnts(entErns:RDD[String],luErns:RDD[String],loErns:RDD[String]) = {
    val luLessEnts = entErns.subtract(luErns)
    val loLessEnts = entErns.subtract(loErns)
    loLessEnts.intersection(loLessEnts)
  }

  def getOrphanLus(lus:RDD[HFileRow], orphanLuErns:RDD[String])(implicit spark: SparkSession) = {
    val orphanLuErnsRows: RDD[(String,String)] = orphanLuErns.map(ern => (ern,ern)) //create tuple of 2 duplicate erns
    val luRows: RDD[(String, (String, String))] = lus.map(row => (row.getCellValue("p_ENT"),(row.key.split("~").head , row.key))) //tuple(ern,(ubrn,row key))
    val joined: RDD[(String, ((String, String), Option[String]))] = luRows.leftOuterJoin(orphanLuErnsRows)
    val orphanLuUbrn = joined.collect { case (ern, ((ubrn,key), None)) => (ern, (ubrn,key)) }
    orphanLuUbrn

  }

    def getOrphanLos(los:RDD[HFileRow], orphanLoErns:RDD[String])(implicit spark: SparkSession) = {
    val orphanLoErnsRows: RDD[(String,String)] = orphanLoErns.map(ern => (ern,ern))
    val loRows: RDD[(String, (String, String))] = los.map(row => (row.getCellValue("ern"),(row.key.split("~").last , row.key)))
    val joined: RDD[(String, ((String, String), Option[String]))] = loRows.leftOuterJoin(orphanLoErnsRows)
    val orphanLoLurn = joined.collect { case (ern, ((lurn,key), None)) => (ern, (lurn,key)) }
      orphanLoLurn

  }



  }
