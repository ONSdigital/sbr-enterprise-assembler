package dao.hive

import global.AppParams
import org.apache.spark.sql._

trait HiveDao {

  def getRegions()(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select postcode,gor as region from ${AppParams.HIVE_DB_NAME}.${AppParams.HIVE_TABLE_NAME}")

  def getRegionsShort()(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select postcodeout,gor as region from ${AppParams.HIVE_DB_NAME}.${AppParams.HIVE_SHORT_TABLE_NAME}")

}

object HiveDao extends HiveDao
