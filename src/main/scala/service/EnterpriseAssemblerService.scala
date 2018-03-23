package service

import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDAO
import global.{AppParams, Configs}
import model.domain.HBaseRow
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager{
  import global.Configs._



  def loadFromJson(appconf:AppParams){
    withSpark{ implicit ss:SparkSession =>
      ParquetDAO.jsonToParquet(PATH_TO_JSON)(ss,appconf)
      ParquetDAO.parquetToHFile(ss,appconf)
    }
    withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf)}
  }

  def readAll(appconf:AppParams) = withSpark{ implicit ss:SparkSession =>
    withHbaseConnection { implicit con: Connection =>
      HBaseDao.readAll(appconf,ss)
    }
  }


  def readWithFilter(appParams:AppParams) = withSpark{ implicit ss:SparkSession =>
    val regex = "72~LEU~"+{appParams.TIME_PERIOD}+"$"
    val lus: RDD[HBaseRow] = HBaseDao.readWithKeyFilter(appParams,regex)

  }


  def readFromHBase(appconf:AppParams) = withHbaseConnection { implicit con: Connection => HBaseDao.readLinksFromHbase(appconf) }

  def deleteFromHFile(appconf:AppParams) = withHbaseConnection { implicit con: Connection => HBaseDao.loadLinksHFile(con,appconf)}

  def loadFromParquet(appconf:AppParams){
    withSpark{ implicit ss:SparkSession => ParquetDAO.parquetToHFile(ss,appconf) }
    withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf) }
  }

  def refreshFromParquet(appconf:AppParams){

    withSpark{ implicit ss:SparkSession => withHbaseConnection { implicit con: Connection =>
      val regex = ".*(?<!~ENT~"+{appconf.TIME_PERIOD}+")$"
      //read existing records from HBase
      val toDelete: RDD[HBaseRow] = HBaseDao.readWithKeyFilter(appconf,regex)
      //delete all rows except ~ENT~ and ~LEU~, but remove all columns from ~LEU~, except 'p_ENT'
      toDelete.sortBy(row => s"${row.key}")
        .flatMap(_.toDeleteHBaseRows(appconf.HBASE_LINKS_COLUMN_FAMILY))
           .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)

      HBaseDao.loadLinksHFile(con,appconf)
    }}
  }

  def cleanExistingRecords(appconf:AppParams){

    withSpark{ implicit ss:SparkSession => withHbaseConnection { implicit con: Connection =>
      val regex = ".*(?<!~ENT~"+{appconf.TIME_PERIOD}+")$"
      //read existing records from HBase
      val toDelete: RDD[HBaseRow] = HBaseDao.readWithKeyFilter(appconf,regex)
      //delete all rows except ~ENT~ and ~LEU~, but remove all columns from ~LEU~, except 'p_ENT'
      toDelete.sortBy(row => s"${row.key}")
        .flatMap(_.toDeleteHBaseRows(appconf.HBASE_LINKS_COLUMN_FAMILY))
           .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)

      HBaseDao.loadLinksHFile(con,appconf)
    }}
  }

  def loadFromHFile(appconf:AppParams) = withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf)}

}
