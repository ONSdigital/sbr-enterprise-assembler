package dao.parquet

import global.AppParams
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import spark.calculations.DataFrameHelper

trait ParquetDao extends DataFrameHelper with Serializable{

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String)(implicit spark:SparkSession,appconf:AppParams) = spark.read.json(jsonFilePath).write.parquet(appconf.PATH_TO_PARQUET)

}
object ParquetDao extends ParquetDao