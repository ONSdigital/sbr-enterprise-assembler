package spark

import global.AppParams
import org.apache.spark.sql.SparkSession



trait SparkSessionManager {

  def withSpark(appconf:AppParams)(doWithinSparkSession: SparkSession => Unit) = {

    implicit val spark: SparkSession = {
      if (appconf.ENV == "cluster") SparkSession.builder().appName("enterprise assembler").getOrCreate()
      else SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
    }

    doWithinSparkSession(spark)

    spark.stop()

  }
}
