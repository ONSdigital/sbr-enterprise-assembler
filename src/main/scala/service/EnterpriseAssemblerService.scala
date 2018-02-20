package service



import connector.HBaseConnector
import converter.ParquetDAO
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession

/**
 *
 */
trait EnterpriseAssemblerService {
import global.Configured._


  val parquetDao = ParquetDAO
  val hbaseDao = HBaseConnector



    def loadFromJson(implicit spark:SparkSession, connection:Connection):Unit  = {
      parquetDao.jsonToParquet(PATH_TO_JSON)
      parquetDao.parquetToHFile
      hbaseDao.loadHFiles
    }


    def loadFromParquet(implicit spark:SparkSession, connection:Connection):Unit  = {
      parquetDao.parquetToHFile
      hbaseDao.loadHFiles
    }

    def loadFromHFile(implicit spark:SparkSession, connection:Connection) = hbaseDao.loadHFiles

}
