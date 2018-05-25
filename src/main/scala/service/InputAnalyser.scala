package service

import dao.hbase.HBaseDao
import global.{AppParams, Configs}
import model.domain.HFileRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.SparkSessionManager
import spark.extensions.sql.parquetRowSchema

case class IsolatedEntitiesReport(orphanCount:Int, orphanKeys:Seq[String])

case class DataReport(entCount:Int, linksCount:Int, lus_Count:Int, childlessEnts:IsolatedEntitiesReport, lusOrphans:IsolatedEntitiesReport, losOrphans:IsolatedEntitiesReport)

object InputAnalyser extends SparkSessionManager{

  def getData(appconf:AppParams) = withSpark(appconf) { implicit spark: SparkSession =>

    val entRdd: RDD[HFileRow] = HBaseDao.readEnterprisesWithKeyFilter(Configs.conf,appconf, ".*~"+{appconf.TIME_PERIOD}+"$")
    entRdd.cache()

    val linksRdd: RDD[HFileRow] = HBaseDao.readLinksWithKeyFilter(Configs.conf,appconf, ".*~"+{appconf.TIME_PERIOD}+"$")
    linksRdd.cache()

    val losRdd: RDD[HFileRow] = HBaseDao.readLouWithKeyFilter(Configs.conf,appconf, ".*~"+{appconf.TIME_PERIOD}+"~.*")
    losRdd.cache()

    val lusRdd: RDD[HFileRow] = linksRdd.collect{ case row if(row.cells.find(_.column == "p_ENT").isDefined) => row }
    lusRdd.cache()


    val entErns = entRdd.map(row => row.key.split("~").head.reverse)
    entErns.cache()

    val luErns = lusRdd.map(row => row.cells.find(_.column == "p_ENT").get.value)
    val loErns: RDD[String] = losRdd.map(row => row.cells.find(_.column == "ern").get.value)

    val luIntersection: RDD[String] = entErns.intersection(luErns.distinct())
    val loIntersection: RDD[String] = entErns.intersection(loErns.distinct())
    val orphanLus: RDD[(String, (String, String))] = getOrphanLus(lusRdd,luIntersection)
    val orphanLos: RDD[(String, (String, String))] = getOrphanLos(losRdd,loIntersection)
    val entsWithoutLus = entErns.subtract(luIntersection)

    entRdd.unpersist()
    linksRdd.unpersist()



  }

  def getOrphanLus(lus:RDD[HFileRow], orphanLuErns:RDD[String])(implicit spark: SparkSession) = {
    val orphanLuErnsRows: RDD[(String,String)] = orphanLuErns.map(ern => (ern,ern))
    val luRows: RDD[(String, (String, String))] = lus.map(row => (row.getCellValue("p_ENT"),(row.key.split("~").head , row.key)))
    val joined: RDD[(String, ((String, String), Option[String]))] = luRows.leftOuterJoin(orphanLuErnsRows)
    val orphanLuUbrn = joined.collect { case (ern, ((ubrn,key), None)) => (ern, (ubrn,key)) }
    orphanLuUbrn

  }

    def getOrphanLos(los:RDD[HFileRow], orphanLoErns:RDD[String])(implicit spark: SparkSession) = {
    val orphanLoErnsRows: RDD[(String,String)] = orphanLoErns.map(ern => (ern,ern))
    val loRows: RDD[(String, (String, String))] = los.map(row => (row.getCellValue("ern"),(row.key.split("~").last , row.key)))
    val joined: RDD[(String, ((String, String), Option[String]))] = loRows.leftOuterJoin(orphanLoErnsRows)
    val orphanLoUbrn = joined.collect { case (ern, ((ubrn,key), None)) => (ern, (ubrn,key)) }
    orphanLoUbrn

  }



  }
