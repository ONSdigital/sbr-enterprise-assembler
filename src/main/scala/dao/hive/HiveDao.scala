package dao.hive


import global.AppParams
import org.apache.spark.sql._



trait HiveDao {
  def getRegions(appConf:AppParams)(implicit spark: SparkSession) =
    spark.sql(s"select postcode,gor as region from ${appConf.HIVE_DB_NAME}.${appConf.REGION_HIVE_TABLE_NAME}")

  def getTph(appConf: AppParams)(implicit spark: SparkSession): DataFrame =
    spark.sql(s"select sic07, tph from ${appConf.HIVE_DB_NAME}.${appConf.TPH_HIVE_TABLE_NAME}")
}

object HiveDao extends HiveDao