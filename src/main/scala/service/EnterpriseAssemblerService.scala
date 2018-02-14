package service



import connector.HBaseConnector
import converter.ParquetDAO
import global.Configured
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession

/**
 *
 */
trait EnterpriseAssemblerService {

  import Configured._

  val parquetDao = ParquetDAO
  val hbaseDao = HBaseConnector



    def loadFromJson(pathToJson:String,pathToParquet:String,pathToHFile:String,namespace:String)(implicit spark:SparkSession, connection:Connection):Unit  = {
      parquetDao.jsonToParquet(pathToJson, pathToParquet)
      parquetDao.parquetToHFile(pathToParquet,pathToHFile)
      hbaseDao.loadHFile(pathToHFile,HBASE_ENTERPRISE_TABLE_NAME, namespace)
    }


    def loadFromJson(pathToJson:String,pathToParquet:String,pathToHFile:String)(implicit spark:SparkSession, connection:Connection):Unit  = {
      parquetDao.jsonToParquet(pathToJson, pathToParquet)
      parquetDao.parquetToHFile(pathToParquet,pathToHFile)
      hbaseDao.loadHFile(pathToHFile,HBASE_ENTERPRISE_TABLE_NAME,HBASE_ENTERPRISE_TABLE_NAMESPACE)
    }



    def loadFromJson(pathToParquetFile:String)(implicit spark:SparkSession, connection:Connection):Unit  = loadFromJson(PATH_TO_JSON,pathToParquetFile,PATH_TO_HFILE)


    def loadFromJson(implicit spark:SparkSession, connection:Connection):Unit  = loadFromJson(PATH_TO_JSON,PATH_TO_PARQUET,PATH_TO_HFILE)


    def loadFromParquet(pathToParquetFile:String,pathToHFile:String = PATH_TO_HFILE)(implicit spark:SparkSession, connection:Connection):Unit  = {
      parquetDao.parquetToHFile(pathToParquetFile,pathToHFile)
      hbaseDao.loadHFile(pathToHFile,HBASE_ENTERPRISE_TABLE_NAME,HBASE_ENTERPRISE_TABLE_NAMESPACE)
    }


    def loadFromParquet(tableName:String,nameSpace:String,pathToParquetFile:String)(implicit spark:SparkSession, connection:Connection):Unit  = {
      parquetDao.parquetToHFile(pathToParquetFile,PATH_TO_HFILE)
      hbaseDao.loadHFile(PATH_TO_HFILE, tableName, nameSpace)
    }



    def loadFromParquet(implicit spark:SparkSession, connection:Connection):Unit = loadFromParquet(PATH_TO_PARQUET,PATH_TO_HFILE)


    def loadFromHFile(pathToHFile:String)(implicit spark:SparkSession, connection:Connection) = hbaseDao.loadHFile(pathToHFile,HBASE_ENTERPRISE_TABLE_NAME)

    def loadFromHFile(implicit spark:SparkSession, connection:Connection) = hbaseDao.loadHFile(PATH_TO_HFILE,HBASE_ENTERPRISE_TABLE_NAME)
}
