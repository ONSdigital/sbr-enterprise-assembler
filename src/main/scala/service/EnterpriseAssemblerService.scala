package service

import connector.HBaseConnector
import converter.DataConverter.parquetToHFile
import org.apache.spark.sql.SparkSession
;

/**
 *
 */
object EnterpriseAssemblerService {

import config.Config._

  def createHFile(pathToJson:String,pathToParquet:String,hfilePath:String)(implicit spark: SparkSession) = {
    import converter.DataConverter._

    jsonToParquet(pathToJson, pathToParquet)
    parquetToHFile(pathToParquet,hfilePath)
  }

  def hfileToHbase(pathTpHFile:String = hfilePath)(implicit spark: SparkSession) = {

    import connector.HBaseConnector._

    HBaseConnector.loadHFile()
    closeConnection
    spark.stop

  }

    def loadFromJson(pathToJsonFile:String,pathToParquetFile:String,pathToHFile:String = hfilePath)(implicit spark:SparkSession):Unit  = {
      createHFile(pathToJsonFile, pathToParquetFile, pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromJson(pathToJsonFile:String)(implicit spark:SparkSession):Unit  = loadFromJson(pathToJsonFile,pathToParquet,hfilePath)

    def loadFromJson(implicit spark:SparkSession):Unit  = loadFromJson(pathToJson,pathToParquet,hfilePath)

    def loadFromParquet(pathToParquetFile:String,pathToHFile:String = hfilePath)(implicit spark:SparkSession):Unit  = {
      parquetToHFile(pathToParquetFile,pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromParquet(implicit spark:SparkSession):Unit = {
      loadFromParquet(pathToParquet,hfilePath)
      hfileToHbase(hfilePath)
    }

    def loadFromHFile(pathToHFile:String = hfilePath)(implicit spark:SparkSession):Unit  = hfileToHbase(pathToHFile)
    def loadFromHFile(implicit spark:SparkSession):Unit = hfileToHbase(hfilePath)

}
