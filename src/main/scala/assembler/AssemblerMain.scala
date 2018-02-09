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
      .appName("enterprise assembler")
      .getOrCreate()

    //loadFromJson
    //loadFromHFile
    loadFromParquet
    //converter.DataConverter.parquetToHFile
    spark.stop()
    //converter.DataConverter.jsonToParquet
  }
}
