package dao.parquet

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import util.configuration.AssemblerConfiguration

object ParquetDao extends Serializable {

  @transient lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")

  def jsonToParquet(jsonFilePath: String)(implicit spark: SparkSession): Unit = {
    spark.read.json(jsonFilePath).write.parquet(AssemblerConfiguration.PathToParquet)
    log.info(s"Wrote parquet file: ${AssemblerConfiguration.PathToParquet}")
  }

}
