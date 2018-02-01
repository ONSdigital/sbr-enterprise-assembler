package main

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  */
class JsonToParquet {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName("enterprise assembler")
    .config("spark.ui.port", "18080")
    .getOrCreate()
  val data: DataFrame = spark.read.json("src/main/resources/sample.json")
  data.write.parquet("src/main/resources/sample.parquet")
}
