package assembler


import org.apache.spark.sql.SparkSession
/**
  *
  */
object AssemblerMain{

  def main(args: Array[String]) {
    import service.EnterpriseAssemblerService._

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("enterprise assembler")
      .config("spark.ui.port", "18080")
      .getOrCreate()

    //loadFromJson
    //loadFromHFile
    //loadFromParquet
    converter.DataConverter.parquetToHFile
    spark.stop()
    //converter.DataConverter.jsonToParquet
  }
}
