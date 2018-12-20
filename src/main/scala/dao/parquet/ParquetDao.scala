package dao.parquet

import global.AppParams
import org.apache.spark.sql.SparkSession

trait ParquetDao extends Serializable {

  def jsonToParquet(jsonFilePath: String)(implicit spark: SparkSession): Unit =
    spark.read.json(jsonFilePath).write.parquet(AppParams.PATH_TO_PARQUET)

}

object ParquetDao extends ParquetDao