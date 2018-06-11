package service

import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDao
import closures.CreateClosures
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager
/**
  *
  */
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager with CreateClosures{
  import global.Configs._


  def createParquetFromJson(appconf:AppParams) = withSpark(appconf) { implicit ss: SparkSession =>

    ParquetDao.jsonToParquet(PATH_TO_JSON)(ss, appconf)
  }


  def loadNewPopulationFromJson(appconf:AppParams) = withSpark(appconf) { implicit ss: SparkSession =>

                                                                 ParquetDao.jsonToParquet(PATH_TO_JSON)(ss, appconf)
    withHbaseConnection { implicit con: Connection => loadFromCreateParquet(appconf)(ss, con)}

                                           }




  def createNewPopulationFromParquet(appconf:AppParams){
    withSpark(appconf){ implicit ss:SparkSession =>
    withHbaseConnection { implicit con: Connection => loadFromCreateParquet(appconf) }
  }}

  def loadFromHFile(appconf:AppParams) = withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf)}





}
