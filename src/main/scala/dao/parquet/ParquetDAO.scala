package dao.parquet

import dao.hbase.converter.WithConversionHelper
import global.{AppParams, Configs}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import spark.calculations.DataFrameHelper


object ParquetDAO extends WithConversionHelper with DataFrameHelper{

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String)(implicit spark:SparkSession,appconf:AppParams) = spark.read.json(jsonFilePath).write.parquet(appconf.PATH_TO_PARQUET)

  def parquetToHFile(implicit spark:SparkSession,appconf:AppParams){

    val appArgs = appconf

    val parquetRDD: RDD[hfile.Tables] = finalCalculations(spark.read.parquet(appconf.PATH_TO_PARQUET), spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.map(row => toEnterpriseRecords(row,appArgs)).cache()

        parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
          .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
              .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

        parquetRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
          .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
              .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

        parquetRDD.unpersist()
  }

    def parquetToRefreshLinksHFileReady(appconf:AppParams)(implicit spark:SparkSession) = {

        val parquetRDD: RDD[(String, hfile.HFileCell)] = spark.read.parquet(appconf.PATH_TO_PARQUET).rdd.flatMap(row => toLinksRefreshRecords(row,appconf))

        parquetRDD.sortBy(t => s"${t._2.key}${t._2.qualifier}")
                                    .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))


      }


}