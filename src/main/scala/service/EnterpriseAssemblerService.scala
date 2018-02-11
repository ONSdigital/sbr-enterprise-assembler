package service

import connector.HBaseConnector
import converter.DataConverter.parquetToHFile
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
;

/**
 *
 */
object EnterpriseAssemblerService {

import global.ApplicationContext._

  def createHFile(implicit spark: SparkSession, connection:Connection) = {
    import converter.DataConverter._

    jsonToParquet(PATH_TO_JSON, PATH_TO_PARQUET)
    parquetToHFile(PATH_TO_PARQUET,PATH_TO_HFILE)
  }

  def createHFile(pathToJson:String,pathToParquet:String,hfilePath:String)(implicit spark: SparkSession, connection:Connection) = {
    import converter.DataConverter._

    jsonToParquet(pathToJson, pathToParquet)
    parquetToHFile(pathToParquet,hfilePath)
  }



  def hfileToHbase(pathTpHFile:String = PATH_TO_HFILE)(implicit connection:Connection) = HBaseConnector.loadHFile(pathTpHFile)



    def loadFromJson(pathToJsonFile:String,pathToParquetFile:String,pathToHFile:String = PATH_TO_HFILE)(implicit spark:SparkSession, connection:Connection):Unit  = {
      createHFile(pathToJsonFile, pathToParquetFile, pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromJson(pathToJsonFile:String)(implicit spark:SparkSession, connection:Connection):Unit  = loadFromJson(pathToJsonFile,PATH_TO_PARQUET,PATH_TO_HFILE)

    def loadFromJson(implicit spark:SparkSession, connection:Connection):Unit  = loadFromJson(PATH_TO_JSON,PATH_TO_PARQUET,PATH_TO_HFILE)

    def loadFromParquet(pathToParquetFile:String,pathToHFile:String = PATH_TO_HFILE)(implicit spark:SparkSession, connection:Connection):Unit  = {
      parquetToHFile(pathToParquetFile,pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromParquet(implicit spark:SparkSession, connection:Connection):Unit = {
      loadFromParquet(PATH_TO_PARQUET,PATH_TO_HFILE)
      hfileToHbase(PATH_TO_HFILE)
    }

    def loadFromHFile(pathToHFile:String = PATH_TO_HFILE)(implicit connection:Connection) = hfileToHbase(pathToHFile)
    def loadFromHFile(implicit spark:SparkSession, connection:Connection) = hfileToHbase(PATH_TO_HFILE)

}
