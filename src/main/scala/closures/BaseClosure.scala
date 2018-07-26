package closures

import dao.hbase.{HBaseDao, HFileUtils}
import global.{AppParams, Configs}
import model.domain.HFileRow
import model.hfile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.RddLogging
import spark.extensions.sql._

trait BaseClosure extends HFileUtils with Serializable with RddLogging{

  val hbaseDao: HBaseDao = HBaseDao

  def getExistingLousDF(appconf: AppParams, confs: Configuration)(implicit spark: SparkSession) = {

    val localUnitsTableName = s"${appconf.HBASE_LOCALUNITS_TABLE_NAMESPACE}:${appconf.HBASE_LOCALUNITS_TABLE_NAME}"
    val regex = ".*~" + {appconf.PREVIOUS_TIME_PERIOD} + "~.*"
    val louHFileRowRdd: RDD[HFileRow] = hbaseDao.readTableWithKeyFilter(confs, appconf, localUnitsTableName, regex)
    val existingLouRdd: RDD[Row] = louHFileRowRdd.map(_.toLouRow)
    spark.createDataFrame(existingLouRdd, louRowSchema)
  }

  def getExistingEntsDF(appconf: AppParams, confs: Configuration)(implicit spark: SparkSession) = {
    val entRegex = ".*~" + {appconf.PREVIOUS_TIME_PERIOD} + "$"
    val entTableName = s"${appconf.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appconf.HBASE_ENTERPRISE_TABLE_NAME}"
    val entHFileRowRdd: RDD[HFileRow] = hbaseDao.readTableWithKeyFilter(confs, appconf, entTableName, entRegex)
    val existingEntRdd: RDD[Row] = entHFileRowRdd.map(_.toEntRow)
    spark.createDataFrame(existingEntRdd, entRowSchema)
  }

  /**
    * returns existing ~LEU~ links DF
    * fields:
    * ubrn, ern, CompanyNo, PayeRefs, VatRefs
    **/
  def getExistingLeusDF(appconf: AppParams, confs: Configuration)(implicit spark: SparkSession) = {
    val linksTableName = s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
    val luRegex = ".*(LEU)~" + {appconf.PREVIOUS_TIME_PERIOD} + "$"
    val existingLinks: RDD[Row] = hbaseDao.readTableWithKeyFilter(confs, appconf, linksTableName, luRegex).map(_.toLuRow)
    val existingLinksDF: DataFrame = spark.createDataFrame(existingLinks, luRowSchema)
    existingLinksDF
  }

  def saveLinks(louDF: DataFrame, leuDF: DataFrame, appconf: AppParams)(implicit spark: SparkSession) = {
    import spark.implicits._
    val lousLinks = louDF.map(row => louToLinks(row, appconf)).flatMap(identity(_)).rdd
    val restOfLinks = leuDF.map(row => leuToLinks(row, appconf)).flatMap(identity(_)).rdd
    val allLinks: RDD[(String, hfile.HFileCell)] = lousLinks.union(restOfLinks)
    allLinks.filter(_._2.value!=null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

  def saveEnts(entsDF: DataFrame, appconf: AppParams)(implicit spark: SparkSession) = {
    val hfileCells: RDD[(String, hfile.HFileCell)] = getEntHFileCells(entsDF, appconf)
    hfileCells.filter(_._2.value!=null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }


  def saveLous(lousDF: DataFrame, appconf: AppParams)(implicit spark: SparkSession) = {
    import spark.implicits._
    val hfileCells = lousDF.map(row => rowToLocalUnit(row, appconf)).flatMap(identity(_)).rdd
    hfileCells.filter(_._2.value!=null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LOCALUNITS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

  def getEntHFileCells(entsDF: DataFrame, appconf: AppParams)(implicit spark: SparkSession): RDD[(String, hfile.HFileCell)] = {
    import spark.implicits._
    val res: RDD[(String, hfile.HFileCell)] = entsDF.map(row => rowToEnt(row, appconf)).flatMap(identity).rdd
    res
  }
}
