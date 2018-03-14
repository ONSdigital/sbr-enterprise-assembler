package dao.parquet

import dao.hbase.converter.WithConversionHelper
import global.Configs
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory



object ParquetDAO extends WithConversionHelper{

  import Configs._

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String)(implicit spark:SparkSession) = spark.read.json(jsonFilePath).write.parquet(PATH_TO_PARQUET)

  def parquetToHFile(implicit spark:SparkSession){


    val parquetRDD = spark.read.parquet(PATH_TO_PARQUET).rdd.map(row => toRecords(row)).cache()

    parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.unpersist()

  }
}
