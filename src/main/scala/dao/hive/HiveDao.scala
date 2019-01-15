package dao.hive

import _root_.util.configuration.AssemblerConfiguration
import org.apache.spark.sql._

object HiveDao {

  def getRegions()(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select postcode,gor as region from ${AssemblerConfiguration.HiveDBName}.${AssemblerConfiguration.HiveTableName}")

  def getRegionsShort()(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select postcodeout,gor as region from ${AssemblerConfiguration.HiveDBName}.${AssemblerConfiguration.HiveShortTableName}")

}

