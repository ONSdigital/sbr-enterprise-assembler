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

  def createHFile(implicit spark: SparkSession) = {
    import converter.DataConverter._

    jsonToParquet(PATH_TO_JSON, PATH_TO_PARQUET)
    parquetToHFile(PATH_TO_PARQUET,PATH_TO_HFILE)
  }

  def createHFile(pathToJson:String,pathToParquet:String,hfilePath:String)(implicit spark: SparkSession) = {
    import converter.DataConverter._

    jsonToParquet(pathToJson, pathToParquet)
    parquetToHFile(pathToParquet,hfilePath)
  }



  def hfileToHbase(pathTpHFile:String = PATH_TO_HFILE)(implicit spark: SparkSession) = {

    import connector.HBaseConnector._

    HBaseConnector.loadHFile(pathTpHFile)
    closeConnection
    spark.stop

  }

    def loadFromJson(pathToJsonFile:String,pathToParquetFile:String,pathToHFile:String = PATH_TO_HFILE)(implicit spark:SparkSession):Unit  = {
      createHFile(pathToJsonFile, pathToParquetFile, pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromJson(pathToJsonFile:String)(implicit spark:SparkSession):Unit  = loadFromJson(pathToJsonFile,PATH_TO_PARQUET,PATH_TO_HFILE)

    def loadFromJson(implicit spark:SparkSession):Unit  = loadFromJson(PATH_TO_JSON,PATH_TO_PARQUET,PATH_TO_HFILE)

    def loadFromParquet(pathToParquetFile:String,pathToHFile:String = PATH_TO_HFILE)(implicit spark:SparkSession):Unit  = {
      parquetToHFile(pathToParquetFile,pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromParquet(implicit spark:SparkSession):Unit = {
      loadFromParquet(PATH_TO_PARQUET,PATH_TO_HFILE)
      hfileToHbase(PATH_TO_HFILE)
    }

    def loadFromHFile(pathToHFile:String = PATH_TO_HFILE)(implicit spark:SparkSession):Unit  = hfileToHbase(pathToHFile)
    def loadFromHFile(implicit spark:SparkSession):Unit = hfileToHbase(PATH_TO_HFILE)

}
