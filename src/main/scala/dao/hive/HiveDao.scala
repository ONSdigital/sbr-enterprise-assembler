package dao.hive

import org.apache.spark.sql._
import _root_.util.configuration.AssemblerConfiguration

trait HiveDao {

  def getRegions()(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select postcode,gor as region from ${AssemblerConfiguration.HiveDBName}.${AssemblerConfiguration.HiveTableName}")

  def getRegionsShort()(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select postcodeout,gor as region from ${AssemblerConfiguration.HiveDBName}.${AssemblerConfiguration.HiveShortTableName}")

}

object HiveDao extends HiveDao
