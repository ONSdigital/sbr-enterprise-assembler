package service

import dao.hbase.HBaseDao
import global.{AppParams, Configs}
import model.domain.HFileRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.RddLogging
import spark.extensions.sql._



case class DataReport(entCount:Long, lusCount:Long, losCount:Long, childlessEntErns:Seq[String], entsWithBrokenkeys:Seq[(String, String)],lusOrphans:Seq[(String,(String,String))], losOrphans:Seq[(String,(String,String))])

object InputAnalyser extends RddLogging{

  def getDfFormatData(appconf:AppParams)(implicit spark: SparkSession):Unit =  {

    /**
      * +----------+----------+------------------+-------------+---------------+----------------+----------+-------------+--------------+--------+-----+------------+
      * |       ern|    entref|              name|trading_style|       address1|        address2|  address3|     address4|      address5|postcode|sic07|legal_status|
      * +----------+----------+------------------+-------------+---------------+----------------+----------+-------------+--------------+--------+-----+------------+
      * |2000000011|9900000009|    INDUSTRIES LTD|            A|     P O BOX 22|INDUSTRIES HOUSE|WHITE LANE|     REDDITCH|WORCESTERSHIRE| B22 2TL|12345|           1|
      * |3000000011|9900000126|BLACKWELLGROUP LTD|            B|COGGESHALL ROAD|     EARLS COLNE|COLCHESTER|         null|          null| CO6 2JX|23456|           1|
      * |4000000011|9900000242|           IBM LTD|            C|     BSTER DEPT|   MAILPOINT A1F|P O BOX 41|NORTH HARBOUR|    PORTSMOUTH| PO6 3AU|34567|           1|
      * +----------+----------+------------------+-------------+---------------+----------------+----------+-------------+--------------+--------+-----+------------+
      * */
    val ents = getEntsDF(appconf)

    /**
      * +------------+----------+---------+--------------+--------------------+
      * |        ubrn|       ern|CompanyNo|      PayeRefs|             VatRefs|
      * +------------+----------+---------+--------------+--------------------+
      * |100000246017|3000000011| 00032262|[1152L, 1153L]|         [111222333]|
      * |100000459235|4000000011| 04223164|       [1166L]|[222666000, 55566...|
      * |100000508723|4000000011| 04223165|[1188L, 1199L]|         [111000111]|
      * |100000508724|4000000011| 00012345|       [5555L]|         [999888777]|
      * |100000827984|3000000011| 00032263|[1154L, 1155L]|         [222333444]|
      * |100002826247|2000000011| 00032261|       [1151L]|         [123123123]|
      * +------------+----------+---------+--------------+--------------------+
      * */

    val lus = getLegalUnitDF(appconf)

    /**
      * +---------+------------+----------+--------------------+----------+------------+-----------------+--------------+----------+-------------+----------+--------+-----+---------+
      * |     lurn|       luref|       ern|                name|    entref|tradingstyle|         address1|      address2|  address3|     address4|  address5|postcode|sic07|employees|
      * +---------+------------+----------+--------------------+----------+------------+-----------------+--------------+----------+-------------+----------+--------+-----+---------+
      * |300000088|100000827984|3000000011|2-ND LU OF BLACKW...|9900000126|        null|North End Rd lane|       Croydon|    Surrey|         null|      null| CR0 1AA| 1122|        2|
      * |300000099|100000246017|3000000011|  BLACKWELLGROUP LTD|9900000126|        null|  COGGESHALL ROAD|   EARLS COLNE|COLCHESTER|         null|      null| CO6 2JX|23456|        2|
      * |400000055|100000508724|4000000011|  3-RD LU OF IBM LTD|9900000242|        null|        IBM HOUSE|  Smile Street|   Cardiff|  SOUTH WALES|      null|CF23 9EU| 3344|        1|
      * |400000066|100000508723|4000000011|  2-ND LU OF IBM LTD|9900000242|        null|          IT DEPT|1 Hight Street|   Newport|  SOUTH WALES|      null|NP10 6XG| 2233|        2|
      * |400000077|100000459235|4000000011|             IBM LTD|9900000242|        null|       BSTER DEPT| MAILPOINT A1F|P O BOX 41|NORTH HARBOUR|PORTSMOUTH| PO6 3AU|34567|        2|
      * +---------+------------+----------+--------------------+----------+------------+-----------------+--------------+----------+-------------+----------+--------+-----+---------+
      *  */
    val lous = getLocallUnitDF(appconf)
    print("Done")
  }

  def getLocallUnitDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val entsRows:RDD[Row] = getLocalUnitsFromLouTable(appconf).map(_.toLouRow)
    spark.createDataFrame(entsRows, louRowSchema)
  }

  def getLegalUnitDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val entsRows:RDD[Row] = getLegalUnitsFromLinks(appconf).map(_.toLeuLinksRow)
    spark.createDataFrame(entsRows, linksLeuRowSchema)
  }

  def getEntsDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val entsRows:RDD[Row] = getEntsFromEntTable(appconf).map(_.toEntRow)
    spark.createDataFrame(entsRows, entRowSchema)
  }

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
