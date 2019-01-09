package dao.parquet

import org.apache.spark.sql.SparkSession
import util.configuration.AssemblerConfiguration

trait ParquetDao extends Serializable {

  def jsonToParquet(jsonFilePath: String)(implicit spark: SparkSession): Unit = {
    spark.read.json(jsonFilePath).write.parquet(AssemblerConfiguration.PathToParquet)
    println(s"Wrote outfile: $jsonFilePath")
  }

}

object ParquetDao extends ParquetDao