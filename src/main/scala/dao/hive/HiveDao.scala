package dao.hive


import global.AppParams
import org.apache.spark.sql._



trait HiveDao {

  def getRegions(appConf:AppParams)(implicit spark: SparkSession) =
    spark.sql(s"select postcode,gor as region from ${appConf.HIVE_DB_NAME}.${appConf.HIVE_TABLE_NAME}")
  def getRegionsShort(appConf:AppParams)(implicit spark: SparkSession) =
    spark.sql(s"select postcodeout,gor as region from ${appConf.HIVE_DB_NAME}.${appConf.HIVE_SHORT_TABLE_NAME}")

  def getRegionsShort(appConf:AppParams)(implicit spark: SparkSession) =
    spark.sql(s"select postcodeout,gor as region from ${appConf.HIVE_DB_NAME}.${appConf.HIVE_SHORT_TABLE_NAME}")

}

object HiveDao extends HiveDao
