package closures

import dao.hbase.HBaseDao
import dao.parquet.ParquetDAO
import global.{AppParams, Configs}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  */
trait CreateClosures {

  def loadFromCreateParquet(appconf:AppParams)(implicit ss:SparkSession,con: Connection){
    ParquetDAO.parquetToHFile(ss,appconf)
    HBaseDao.loadHFiles(con,appconf)
  }


  def createSingleRefreshHFile(appconf: AppParams)(implicit ss: SparkSession) = {
    val localConfigs = Configs.conf
    val cleanRecs: RDD[(String, hfile.HFileCell)] = HBaseDao.readLinksWithKeyFilter(localConfigs,appconf, ".*(?<!~ENT~" + {
      appconf.TIME_PERIOD
    } + ")$").flatMap(_.toDeleteHFileRows(appconf.HBASE_LINKS_COLUMN_FAMILY)).sortBy(v => {
      s"${v._2.key}${v._2.qualifier}"
    })

    val updateRecs: RDD[(String, hfile.HFileCell)] = ss.read.parquet(appconf.PATH_TO_PARQUET).rdd.flatMap(row => ParquetDAO.toLinksRefreshRecords(row, appconf)).sortBy(v => {
      s"${v._2.key}${v._2.qualifier}"
    })

    val totalHFileRefresh = (cleanRecs ++ updateRecs).distinct().repartition(cleanRecs.getNumPartitions)
    val sorted: RDD[(String, hfile.HFileCell)] = totalHFileRefresh.sortBy(v => {
      s"${v._2.key}${v._2.qualifier}${v._2.kvType}"
    }).cache()

    val collected = sorted.collect()
    sorted.unpersist()

    val ready: RDD[(ImmutableBytesWritable, KeyValue)] = sorted.map(data => (new ImmutableBytesWritable(data._1.getBytes()), data._2.toKeyValue))


    ready.saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

}