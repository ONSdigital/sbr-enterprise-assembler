package spark

import global.AppParams
import org.apache.spark.sql.SparkSession

trait  SparkSessionManager {

  def withSpark(doWithinSparkSession: SparkSession => Unit): Unit = {

    implicit val spark: SparkSession = {
      if (AppParams.ENV == "cluster") SparkSession.builder().appName("enterprise assembler").enableHiveSupport().getOrCreate()
      else SparkSession.builder().master("local[8]").appName("enterprise assembler").getOrCreate()
    }

    doWithinSparkSession(spark)

    spark.stop()

  }
}
