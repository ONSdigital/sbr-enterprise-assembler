package main


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  */
object AssemblerMain extends App{

  override def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("Spark SQL basic example")
      .config("spark.ui.port", "18080")
      .getOrCreate()
    import spark.implicits._
/*    val data = spark.read.json("src/main/resources/sample.json")///Users/VLAD/dev/ons/enterprise-assemble/src/main/resources/sample.csv
    data.printSchema()
    data.write.parquet("src/main/resources/sample.parquet")*/
    val parquetFileDF = spark.read.parquet("src/main/resources/sample.parquet")
    parquetFileDF.createOrReplaceTempView("businessIndexRec")
    val namesDF = spark.sql("SELECT BusinessName FROM businessIndexRec WHERE BusinessName LIKE '%TEST%'")
    namesDF.show()
  }
}
