package service

import dao.hbase.converter.WithConversionHelper
import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDAO
import dao.parquet.ParquetDAO.{finalCalculations, toEnterpriseRecords, toLinksRefreshRecords}
import global.{AppParams, Configs}
import model.domain.HFileRow
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.SparkSessionManager
import org.apache.spark.sql.Row
import spark.extensions.sql.SqlRowExtensions
/**
  *
  */
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager with WithConversionHelper{
  import global.Configs._



  def loadFromJson(appconf:AppParams){
    withSpark{ implicit ss:SparkSession =>
      ParquetDAO.jsonToParquet(PATH_TO_JSON)(ss,appconf)
      ParquetDAO.parquetToHFile(ss,appconf)
    }
    withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf)}
  }


  def loadFromParquet(appconf:AppParams){
    withSpark{ implicit ss:SparkSession => ParquetDAO.parquetToHFile(ss,appconf) }
    withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf) }
  }

  def loadRefresh(appconf:AppParams) = {
          createDeleteLinksHFile(appconf)
          createUpdateLinksHFileFromParquet(appconf)
          loadRefreshFromHFiles(appconf)
  }

  def createRefreshHFiles(appconf:AppParams) = withSpark{ implicit spark:SparkSession => withHbaseConnection { implicit con: Connection =>

    val refreshDF = spark.read.parquet(appconf.PATH_TO_PARQUET).cache()
//do links :
    val parquetRDD: RDD[(String, hfile.HFileCell)] = refreshDF.rdd.flatMap(row => toLinksRefreshRecords(row,appconf))

    parquetRDD.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))

    parquetRDD.saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE_UPDATE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)


    case class ErnDF(id:String, ern:Long)

    val regex = ".*(?<!~ENT~"+{appconf.TIME_PERIOD}+")$"
    //read existing records from HBase
    val toDelete: RDD[HFileRow] = HBaseDao.readWithKeyFilter(appconf,regex).cache()
    //delete all rows except ~ENT~ and ~LEU~, and remove all columns from ~LEU~, except 'p_ENT'
    toDelete.sortBy(row => s"${row.key}")
      .flatMap(_.toDeleteHFileEntries(appconf.HBASE_LINKS_COLUMN_FAMILY))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE_DELETE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)


//do Enterprises
    val entToLuMap: RDD[ErnDF] = toDelete.filter(r => r.key.endsWith("~LEU~"+{appconf.TIME_PERIOD})).map(lu => {
      ErnDF(lu.getId, lu.cells.find(_.column == "p_ENT").get.value.toLong)
    })

    val erns: DataFrame = spark.createDataFrame(entToLuMap)

    val fullLUs = refreshDF.join(erns,"id")

    val entsRDD: RDD[(String, hfile.HFileCell)] = finalCalculations(spark.read.parquet(appconf.PATH_TO_PARQUET), spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.flatMap(row => rowToEnterprise(row,row.getString("ern").get,appconf))

    entsRDD.sortBy(t => s"${t._2.key}${t._2.qualifier}").map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    refreshDF.unpersist()
    toDelete.unpersist()
  }}

  def createUpdateLinksHFileFromParquet(appconf:AppParams) = withSpark{ implicit ss:SparkSession =>
    ParquetDAO.parquetToRefreshLinksHFileReady(appconf)
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE_UPDATE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

  def createDeleteLinksHFile(appconf:AppParams) = withSpark{ implicit ss:SparkSession => withHbaseConnection { implicit con: Connection =>
      val regex = ".*(?<!~ENT~"+{appconf.TIME_PERIOD}+")$"
      //read existing records from HBase
      val toDelete: RDD[HFileRow] = HBaseDao.readWithKeyFilter(appconf,regex)
      //delete all rows except ~ENT~ and ~LEU~, and remove all columns from ~LEU~, except 'p_ENT'
      toDelete.sortBy(row => s"${row.key}")
        .flatMap(_.toDeleteHFileEntries(appconf.HBASE_LINKS_COLUMN_FAMILY))
           .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE_DELETE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
    }}


  def loadFromHFile(appconf:AppParams) = withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf)}

  def loadRefreshFromHFiles(appconf:AppParams) =  withSpark{ implicit ss:SparkSession => withHbaseConnection { implicit con: Connection =>

    HBaseDao.loadDeleteHFile(con,appconf)
    HBaseDao.loadRefreshHFile(con,appconf)

  }}


  def createSingleRefreshHFile(appconf:AppParams) = withSpark{ implicit ss:SparkSession => withHbaseConnection { implicit con: Connection =>

    val cleanRecs: RDD[(String, hfile.HFileCell)] = HBaseDao.readWithKeyFilter(appconf, ".*(?<!~ENT~" + {appconf.TIME_PERIOD} + ")$").flatMap(_.toDeleteHFileRows(appconf.HBASE_LINKS_COLUMN_FAMILY)).sortBy(v => {
      s"${v._2.key}${v._2.qualifier}"
    })

    val updateRecs: RDD[(String, hfile.HFileCell)] = ss.read.parquet(appconf.PATH_TO_PARQUET).rdd.flatMap(row => toLinksRefreshRecords(row,appconf)).sortBy(v => {
      s"${v._2.key}${v._2.qualifier}"
    })

    val totalHFileRefresh = (cleanRecs++updateRecs).distinct().repartition(cleanRecs.getNumPartitions)
    val sorted: RDD[(String, hfile.HFileCell)] = totalHFileRefresh.sortBy(v => {
      s"${v._2.key}${v._2.qualifier}${v._2.kvType}"
    }).cache()

    val collected = sorted.collect()
    sorted.unpersist()

    val ready: RDD[(ImmutableBytesWritable, KeyValue)] = sorted.map(data => (new ImmutableBytesWritable(data._1.getBytes()), data._2.toKeyValue))


    ready.saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }}
}
