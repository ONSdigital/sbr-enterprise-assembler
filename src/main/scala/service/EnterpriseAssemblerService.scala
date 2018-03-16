package service

import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDAO
import dao.parquet.ParquetDAO.getId
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager{
  import global.Configs._

  def loadDeleteFromJson{
    withSpark{ implicit spark: SparkSession =>
      ParquetDAO.jsonToParquet(PATH_TO_JSON)
      //ParquetDAO.toDeleteLinksHFile

     withHbaseConnection { implicit connection: Connection =>
       spark.read.parquet(PATH_TO_PARQUET).rdd.map(row => {
         val id = getId(row).getBytes
         HBaseDao.deleteRow(id)
       })
  }}}


  def loadDeletesFromParquet{
    withHbaseConnection { implicit connection: Connection =>
      withSpark{ implicit spark: SparkSession =>
     val parquetRdd: RDD[Row] = spark.read.parquet(PATH_TO_PARQUET).rdd
       parquetRdd.map(row => {
         val id = getId(row).getBytes
         HBaseDao.deleteRow(id)
       })
  }  }}




  def loadRefreshFromJson{
    withSpark{ implicit SparkSession =>
      ParquetDAO.jsonToParquet(PATH_TO_JSON)
      ParquetDAO.parquetToRefreshHFile
    }
    withHbaseConnection { implicit connection: Connection => HBaseDao.loadLinksHFile }
  }

  def loadFromJson{
    withSpark{ implicit SparkSession =>
      ParquetDAO.jsonToParquet(PATH_TO_JSON)
      ParquetDAO.parquetToHFile
    }
    withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}
  }


  def loadFromParquet{
    withSpark{ implicit SparkSession => ParquetDAO.parquetToHFile }
    withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles }
  }

  def refreshFromParquet: Unit ={
    withSpark{ implicit SparkSession => ParquetDAO.parquetToRefreshHFile }
    withHbaseConnection { implicit connection: Connection => HBaseDao.loadLinksHFile }

  }

  def loadFromHFile = withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}

}
