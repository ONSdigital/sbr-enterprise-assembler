package dao.spark

import org.apache.spark.sql.SparkSession
import util.configuration.AssemblerConfiguration

object  SparkSessionManager {

  def withSpark(doWithinSparkSession: SparkSession => Unit): Unit = {

    implicit val spark: SparkSession = {
      if (AssemblerConfiguration.inCluster)
        SparkSession.builder().appName("enterprise assembler").enableHiveSupport().getOrCreate()
      else
        SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
    }

    doWithinSparkSession(spark)

    spark.stop()

  }
}
