package service



import connector.HBaseConnector
import converter.ParquetDAO
import hbase.ConnectionManagement
import org.apache.hadoop.hbase.client.Connection
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseAssemblerService extends ConnectionManagement with SparkSessionManager{
  import global.Configured._


  val parquetDao = ParquetDAO
  val hbaseDao = HBaseConnector



  def loadFromJson:Unit  = {
    withSpark{ implicit SparkSession =>
                parquetDao.jsonToParquet(PATH_TO_JSON)
                parquetDao.parquetToHFile
    }
    withHbaseConnection { implicit connection: Connection => hbaseDao.loadHFiles}
  }


  def loadFromParquet:Unit  = {
    withSpark{ implicit SparkSession => parquetDao.parquetToHFile}
    withHbaseConnection { implicit connection: Connection => hbaseDao.loadHFiles}
  }

  def loadFromHFile = withHbaseConnection { implicit connection: Connection => hbaseDao.loadHFiles}

}
