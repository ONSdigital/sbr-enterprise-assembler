package dao.hive


import global.AppParams
import org.apache.spark.sql._
import org.apache.spark.sql.hive._


trait HiveDao {
  def getRegions(appConf: AppParams)(implicit spark: SparkSession) = {
  val sqlContext = new HiveContext(spark.sparkContext)
  sqlContext.sql("select postcode,gor as region from ${HIVE_DB_NAME}.{$HIVE_TABLE_NAME}")
  //spark.sql(s"select postcode,gor as region from ${appConf.HIVE_DB_NAME}.${appConf.HIVE_TABLE_NAME}")
}

}

object HiveDao extends HiveDao