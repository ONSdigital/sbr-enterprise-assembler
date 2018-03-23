package dao.parquet

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import spark.calculations.DataFrameHelper
import global.{AppParams, Configs}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.filter.RegexStringComparator


object ParquetDAO extends WithConversionHelper with DataFrameHelper{

  import Configs._

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String)(implicit spark:SparkSession,appconf:AppParams) = spark.read.json(jsonFilePath).write.parquet(appconf.PATH_TO_PARQUET)

  def parquetToHFile(implicit spark:SparkSession,appconf:AppParams){

    val appArgs = appconf

    val parquetRDD: RDD[hfile.Tables] = finalCalculations(spark.read.parquet(appconf.PATH_TO_PARQUET), spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.map(row => toEnterpriseRecords(row,appArgs)).cache()

    //val parquetRDDreduceHfile = parquetRDD.coalesce(appconf.HFILE_TOTAL_COUNT.toInt)

    parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.unpersist()
  }

  def parquetToDeleteHFile(appconf:AppParams)(implicit spark:SparkSession,conn:Connection)  {

    val appArgs = appconf
    val rowsToDelete: Unit = HBaseDao.readLinksFromHbase(appconf)

    val parquetRDD: RDD[hfile.Tables] = finalCalculations(spark.read.parquet(appconf.PATH_TO_PARQUET), spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.map(row => toEnterpriseRecords(row, appArgs)).cache()
    //val regex = "~LEU~"+{appconf.TIME_PERIOD}+"$"
    val regex = ".*(?<!~LEU~"+{appconf.TIME_PERIOD}+")$"
    HBaseDao.setScanner(regex,appconf)
    //val parquetRDDreduceHfile = parquetRDD.coalesce(appconf.HFILE_TOTAL_COUNT.toInt)

    parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toDeleteKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

    def toRefreshLinksHFile(implicit spark:SparkSession, appconf:AppParams) {

        val appArgs = appconf

        val parquetRDD: RDD[hfile.Tables] = finalCalculations(spark.read.parquet(appconf.PATH_TO_PARQUET), spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.map(row => toEnterpriseRecords(row,appArgs)).cache()

        //val parquetRDDreduceHfile = parquetRDD.coalesce(appconf.HFILE_TOTAL_COUNT.toInt)

        parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
          .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toDeleteKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)


    //++++++++++++++++++++++++++++++++++++++++++++++++++++

/*    val rr: RDD[(ImmutableBytesWritable, KeyValue)] = spark.read.parquet(appconf.PATH_TO_PARQUET).rdd.map(getId(_)).map(id => {
      val idBytes = id.getBytes
      (new ImmutableBytesWritable(idBytes), new KeyValue(idBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Delete)) })


    rr.saveAsNewAPIHadoopFile(
      appconf.PATH_TO_LINKS_HFILE,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      Configs.conf
    )*/
  }


}