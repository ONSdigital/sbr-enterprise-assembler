package service

import connector.HBaseConnector
import converter.DataConverter.parquetToHFile
import org.apache.spark.sql.SparkSession
;

/**
 *
 */
object EnterpriseAssemblerService {

import global.ApplicationContext._

  def createHFile(pathToJson:String,pathToParquet:String,hfilePath:String)(implicit spark: SparkSession) = {
    import converter.DataConverter._

    jsonToParquet(pathToJson, pathToParquet)
    parquetToHFile(pathToParquet,hfilePath)
  }

  def hfileToHbase(pathTpHFile:String = config.getString("files.hfile"))(implicit spark: SparkSession) = {

    import connector.HBaseConnector._

    HBaseConnector.loadHFile(pathTpHFile)
    closeConnection
    spark.stop

  }

    def loadFromJson(pathToJsonFile:String,pathToParquetFile:String,pathToHFile:String = config.getString("files.hfile"))(implicit spark:SparkSession):Unit  = {
      createHFile(pathToJsonFile, pathToParquetFile, pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromJson(pathToJsonFile:String)(implicit spark:SparkSession):Unit  = loadFromJson(pathToJsonFile,config.getString("files.parquet"),config.getString("files.hfile"))

    def loadFromJson(implicit spark:SparkSession):Unit  = loadFromJson(config.getString("files.json"),config.getString("files.parquet"),config.getString("files.hfile"))

    def loadFromParquet(pathToParquetFile:String,pathToHFile:String = config.getString("files.hfile"))(implicit spark:SparkSession):Unit  = {
      parquetToHFile(pathToParquetFile,pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromParquet(implicit spark:SparkSession):Unit = {
      loadFromParquet(config.getString("files.parquet"),config.getString("files.hfile"))
      hfileToHbase(config.getString("files.hfile"))
    }

    def loadFromHFile(pathToHFile:String = config.getString("files.hfile"))(implicit spark:SparkSession):Unit  = hfileToHbase(pathToHFile)
    def loadFromHFile(implicit spark:SparkSession):Unit = hfileToHbase(config.getString("files.hfile"))

}
