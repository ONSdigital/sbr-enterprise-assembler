package dao.parquet

import org.apache.spark.sql.SparkSession
import util.options.ConfigOptions

trait ParquetDao extends Serializable {

  def jsonToParquet(jsonFilePath: String)(implicit spark: SparkSession): Unit =
    spark.read.json(jsonFilePath).write.parquet(ConfigOptions.BIFilePath)

}

object ParquetDao extends ParquetDao