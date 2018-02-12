package service



import connector.HBaseConnector
import converter.ParquetDAO
import global.Configured
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession

/**
 *
 */
trait EnterpriseAssemblerService extends Configured{ this:Configured =>

  import ParquetDAO._

  def createHFile(implicit spark: SparkSession, connection:Connection) = {

    jsonToParquet(PATH_TO_JSON, PATH_TO_PARQUET)
    parquetToHFile(PATH_TO_PARQUET,PATH_TO_HFILE)
  }

  def createHFile(pathToJson:String,pathToParquet:String,hfilePath:String)(implicit spark: SparkSession, connection:Connection) = {

    jsonToParquet(pathToJson, pathToParquet)
    parquetToHFile(pathToParquet,hfilePath)
  }



  def hfileToHbase(pathTpHFile:String = PATH_TO_HFILE)(implicit connection:Connection) = HBaseConnector.loadHFile(pathTpHFile,HBASE_ENTERPRISE_TABLE_NAME)



    def loadFromJson(pathToJsonFile:String,pathToParquetFile:String,pathToHFile:String)(implicit spark:SparkSession, connection:Connection):Unit  = {
      createHFile(pathToJsonFile, pathToParquetFile, pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromJson(implicit spark:SparkSession, connection:Connection):Unit  = loadFromJson(PATH_TO_JSON,PATH_TO_PARQUET,PATH_TO_HFILE)

    def loadFromParquet(pathToParquetFile:String,pathToHFile:String)(implicit spark:SparkSession, connection:Connection):Unit  = {
      parquetToHFile(pathToParquetFile,pathToHFile)
      hfileToHbase(pathToHFile)
    }

    def loadFromParquet(implicit spark:SparkSession, connection:Connection):Unit = {
      loadFromParquet(PATH_TO_PARQUET,PATH_TO_HFILE)
      hfileToHbase(PATH_TO_HFILE)
    }

    def loadFromHFile(pathToHFile:String)(implicit connection:Connection) = hfileToHbase(pathToHFile)

    def loadFromHFile(implicit spark:SparkSession, connection:Connection) = hfileToHbase(PATH_TO_HFILE)

}
