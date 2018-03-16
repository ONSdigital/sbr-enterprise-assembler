package dao.parquet

import dao.hbase.converter.WithConversionHelper
import spark.calculations.DataFrameHelper
import global.Configs
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object ParquetDAO extends WithConversionHelper with DataFrameHelper{

  import Configs._

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String)(implicit spark:SparkSession) = spark.read.json(jsonFilePath).write.parquet(PATH_TO_PARQUET)

  def parquetToHFile(implicit spark:SparkSession){

    val parquetRDD: RDD[hfile.Tables] = finalCalculations(spark.read.parquet(PATH_TO_PARQUET), spark.read.option("header", "true").csv(PATH_TO_PAYE)).rdd.map(row => toEnterpriseRecords(row)).cache()

    parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.unpersist()
  }

  def toDeleteLinksHFile(implicit spark:SparkSession) {

    val rr: RDD[(ImmutableBytesWritable, KeyValue)] = spark.read.parquet(PATH_TO_PARQUET).rdd.map(row => {
      val id = getId(row).getBytes
      (new ImmutableBytesWritable(id), new KeyValue(id, HBASE_LINKS_COLUMN_FAMILY.getBytes, "blx122344545rrgh".getBytes, System.currentTimeMillis(), KeyValue.Type.Delete)) }).sortBy(t => s"${t._2.getKey}")


    rr.saveAsNewAPIHadoopFile(
      PATH_TO_LINKS_HFILE,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      Configs.conf
    )
  }


  def parquetToRefreshHFile(implicit spark:SparkSession){

    val rdd: RDD[(ImmutableBytesWritable, KeyValue)] = finalCalculations(spark.read.parquet(PATH_TO_PARQUET), spark.read.option("header", "true").csv(PATH_TO_PAYE)).rdd.flatMap(row => toLuRecords(row))
           .sortBy(t => s"${t._2.key}${t._2.qualifier}")
                               .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))

                                                          rdd.saveAsNewAPIHadoopFile(
                                                                                  PATH_TO_LINKS_HFILE,
                                                                                  classOf[ImmutableBytesWritable],
                                                                                  classOf[KeyValue],
                                                                                  classOf[HFileOutputFormat2],
                                                                                  Configs.conf
                                                                                )

  }


}