package dao.hive

import org.apache.spark.sql._
import _root_.util.options.ConfigOptions

trait HiveDao {

  def getRegions()(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select postcode,gor as region from ${ConfigOptions.HiveDBName}.${ConfigOptions.HiveTableName}")

  def getRegionsShort()(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select postcodeout,gor as region from ${ConfigOptions.HiveDBName}.${ConfigOptions.HiveShortTableName}")

}

object HiveDao extends HiveDao
