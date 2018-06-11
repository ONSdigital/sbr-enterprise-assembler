package service

import dao.hbase.HBaseDao
import global.{AppParams, Configs}
import model.domain.HFileRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spark.{RddLogging, SparkSessionManager}



case class DataReport(entCount:Long, lusCount:Long, losCount:Long, childlessEntErns:Seq[String], entsWithBrokenkeys:Seq[(String, String)],lusOrphans:Seq[(String,(String,String))], losOrphans:Seq[(String,(String,String))])

object InputAnalyser extends RddLogging{

  def getData(appconf:AppParams)(implicit spark: SparkSession):DataReport =  {

    val entRdd: RDD[HFileRow] = getRepartionedRdd(getEntsFromEntTable(appconf))
    //printRdd("ENT RDD",entRdd,"HFileRow")
    entRdd.cache()



    //printRdd("enterprises",entRdd,"HFileRow")
    val lusRdd: RDD[HFileRow] = getRepartionedRdd(getLegalUnitsFromLinks(appconf))
    lusRdd.cache()
    //printRdd("LEU",lusRdd,"HFileRow")

    val losRdd: RDD[HFileRow] = getRepartionedRdd(getLocalUnitsFromLouTable(appconf))
    losRdd.cache()

    //printRdd("LOU",losRdd,"HFileRow")

    val entErns = entRdd.map(row => row.cells.find(_.column=="ern").get.value)
    entErns.cache()
    val entsWithKeyDiscrepancies: RDD[(String, String)] = entRdd.collect{case row if(row.key.split("~").head.reverse != row.cells.find(_.column=="ern").get.value) => (row.key,row.cells.find(_.column=="ern").get.value)}
    //printRdd("entsWithKeyDiscrepancies",entRdd,"(String,String)")


    val luErns: RDD[String] = getRepartionedRdd(lusRdd.map(row => row.cells.find(_.column == "p_ENT").get.value).distinct())
    luErns.cache()
    val loErns: RDD[String] = getRepartionedRdd(losRdd.map(row => row.cells.find(_.column == "ern").get.value).distinct())
    loErns.cache()
    val luIntersection: RDD[String] = entErns.intersection(luErns)
    val loIntersection: RDD[String] = entErns.intersection(loErns)
    val orphanLus: RDD[(String, (String, String))] = getOrphanLus(lusRdd,luIntersection)
    val orphanLos: RDD[(String, (String, String))] = getOrphanLos(losRdd,loIntersection)
    val childlessEnts = getChildlessEnts(entErns,luErns,loErns)

    val entCount = entRdd.count()
    val res = DataReport(entCount,lusRdd.count(),losRdd.count(),childlessEnts.collect(), entsWithKeyDiscrepancies.collect(),orphanLus.collect(),orphanLos.collect())

    entErns.unpersist()
    entRdd.unpersist()
    lusRdd.unpersist()
    losRdd.unpersist()
    luErns.unpersist()
    loErns.unpersist()

    res
  }

  private def getEntsFromEntTable(appconf: AppParams)(implicit ss:SparkSession) = {
    HBaseDao.readEnterprisesWithKeyFilter(Configs.conf, appconf, ".*~" + {
      appconf.TIME_PERIOD
    } + "$")
  }

  private def getLocalUnitsFromLouTable(appconf: AppParams)(implicit ss:SparkSession) = {
    HBaseDao.readLouWithKeyFilter(Configs.conf, appconf, ".*~" + {
      appconf.TIME_PERIOD
    } + "~.*")
  }

  private def getLegalUnitsFromLinks(appconf: AppParams)(implicit ss:SparkSession) = {
    HBaseDao.readLinksWithKeyFilter(Configs.conf, appconf, ".*~LEU~" + {
      appconf.TIME_PERIOD
    } + "$")
      .collect { case row if (row.cells.find(_.column == "p_ENT").isDefined) => row }
  }

  def getRepartionedRdd[T](rdd:RDD[T]) = {
    val noOfPartiions = rdd.getNumPartitions
    rdd.repartition(noOfPartiions)
    rdd
  }

  def getChildlessEnts(entErns:RDD[String],luErns:RDD[String],loErns:RDD[String]) = {
    val luLessEnts = entErns.subtract(luErns)
    val loLessEnts = entErns.subtract(loErns)
    loLessEnts.intersection(luLessEnts)
  }

  def getOrphanLus(lus:RDD[HFileRow], orphanLuErns:RDD[String])(implicit spark: SparkSession) = {
    val numberOfPartitions = lus.getNumPartitions
    val orphanLuErnsRows: RDD[(String,String)] = getRepartionedRdd(orphanLuErns.map(ern => (ern,ern))) //create tuple of 2 duplicate erns
    val luRows: RDD[(String, (String, String))] = getRepartionedRdd(lus.map(row => (row.getCellValue("p_ENT"),(row.key.split("~").head , row.key))) )//tuple(ern,(ubrn,row key))
    val joined: RDD[(String, ((String, String), Option[String]))] = luRows.leftOuterJoin(orphanLuErnsRows)
    val orphanLuUbrn = joined.collect { case (ern, ((ubrn,key), None)) => (ern, (ubrn,key)) }
    orphanLuUbrn.coalesce(numberOfPartitions)

  }

    def getOrphanLos(los:RDD[HFileRow], orphanLoErns:RDD[String])(implicit spark: SparkSession) = {
      val numberOfPartitions = los.getNumPartitions
      val orphanLoErnsRows: RDD[(String,String)] = getRepartionedRdd(orphanLoErns.map(ern => (ern,ern)))
      val loRows: RDD[(String, (String, String))] = getRepartionedRdd(los.map(row => (row.getCellValue("ern"),(row.key.split("~").last , row.key))))
      val joined: RDD[(String, ((String, String), Option[String]))] = loRows.leftOuterJoin(orphanLoErnsRows)
      val orphanLoLurn = joined.collect { case (ern, ((lurn,key), None)) => (ern, (lurn,key)) }
      orphanLoLurn.coalesce(numberOfPartitions)

  }



}
