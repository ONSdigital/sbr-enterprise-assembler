package spark

import org.apache.spark.sql.SparkSession
import util.options.ConfigOptions

trait  SparkSessionManager {

  def withSpark(doWithinSparkSession: SparkSession => Unit): Unit = {

    implicit val spark: SparkSession = {
      if (ConfigOptions.inCluster)
        SparkSession.builder().appName("enterprise assembler").enableHiveSupport().getOrCreate()
      else
        SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
    }

    doWithinSparkSession(spark)

    spark.stop()

  }
}
