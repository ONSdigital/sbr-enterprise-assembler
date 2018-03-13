package service

import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDAO
import org.apache.hadoop.hbase.client.Connection
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager{
  import global.Configs._

  def loadFromJson(timePeriod:String){
    withSpark{ implicit SparkSession =>
      ParquetDAO.jsonToParquet(PATH_TO_JSON)
      ParquetDAO.parquetToHFile(timePeriod)
    }
    //withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}
  }


  def loadFromParquet(timePeriod:String){
    withSpark{ implicit SparkSession => ParquetDAO.parquetToHFile(timePeriod) }
    withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles }
  }

  def loadFromHFile = withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}

}
