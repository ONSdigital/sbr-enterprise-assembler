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

  def loadFromJson{
    withSpark{ implicit SparkSession =>
      ParquetDAO.jsonToParquet(PATH_TO_JSON)
      ParquetDAO.parquetToHFile
    }
    withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}
  }


  def loadFromParquet{
    withSpark{ implicit SparkSession => ParquetDAO.parquetToHFile }
    //withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles }
  }

  def loadFromHFile = withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}

}
