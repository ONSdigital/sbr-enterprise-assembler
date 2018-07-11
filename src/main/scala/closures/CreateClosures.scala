package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import global.{AppParams, Configs}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import spark.RddLogging
import spark.calculations.DataFrameHelper
import spark.extensions.sql._

/**
  *
  */
trait CreateClosures extends WithConversionHelper with DataFrameHelper with RddLogging with Serializable{


  def parquetCreateNewToHFile(implicit spark:SparkSession, appconf:AppParams){

    val appArgs = appconf

    val payeDF = spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)
    val vatDF  = spark.read.option("header", "true").csv(appconf.PATH_TO_VAT)

    val stringifiedParquet = spark.read.parquet(appArgs.PATH_TO_PARQUET).castAllToString
    val parquetDF = adminCalculations(stringifiedParquet, payeDF, vatDF)
    //printDF("parquetDF", parquetDF)
    val parquetRDD = parquetDF.rdd.map(row => toNewEnterpriseRecordsWithLou(row,appArgs)).cache()

    parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.flatMap(_.localUnits).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LOCALUNITS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)



    parquetRDD.unpersist()
  }


  def createSingleRefreshHFile(appconf: AppParams)(implicit ss: SparkSession) = {
    val localConfigs = Configs.conf
    val cleanRecs: RDD[(String, hfile.HFileCell)] = HBaseDao.readLinksWithKeyFilter(localConfigs,appconf, ".*(?<!~ENT~" + {
      appconf.TIME_PERIOD
    } + ")$").flatMap(_.toDeleteHFileRows(appconf.HBASE_LINKS_COLUMN_FAMILY)).sortBy(v => {
      s"${v._2.key}${v._2.qualifier}"
    })

    val updateRecs: RDD[(String, hfile.HFileCell)] = ss.read.parquet(appconf.PATH_TO_PARQUET).rdd.flatMap(row => toLinksRefreshRecords(row, appconf)).sortBy(v => {
      s"${v._2.key}${v._2.qualifier}"
    })

    val totalHFileRefresh = (cleanRecs ++ updateRecs).distinct().repartition(cleanRecs.getNumPartitions)
    val sorted: RDD[(String, hfile.HFileCell)] = totalHFileRefresh.sortBy(v => {
      s"${v._2.key}${v._2.qualifier}${v._2.kvType}"
    }).cache()

    //val collected = sorted.collect()
    sorted.unpersist()

    val ready: RDD[(ImmutableBytesWritable, KeyValue)] = sorted.map(data => (new ImmutableBytesWritable(data._1.getBytes()), data._2.toKeyValue))


    ready.saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

}

object CreateClosures extends CreateClosures