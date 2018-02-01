package main

import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  *
  */
object JsonToParquet {

  def convertAndSave(spark: SparkSession) {
    val data: DataFrame = spark.read.json("src/main/resources/sample.json")
    data.write.parquet("src/main/resources/sample.parquet")
  }
}
