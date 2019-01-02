package dao.parquet

import org.apache.spark.sql.SparkSession
import util.options.ConfigOptions

trait ParquetDao extends Serializable {

  def jsonToParquet(jsonFilePath: String)(implicit spark: SparkSession): Unit = {
    spark.read.json(jsonFilePath).write.parquet(ConfigOptions.PathToParquet)
    println(s"Wrote outfile: $jsonFilePath")
  }

}

object ParquetDao extends ParquetDao