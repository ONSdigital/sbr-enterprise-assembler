package assembler


import connector.HBaseConnector
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
    //loadFromParquet
    //converter.DataConverter.parquetToHFile
    loadFromParquet
    //converter.DataConverter.parquetToHFile
    spark.stop()
    HBaseConnector.closeConnection
    //converter.DataConverter.jsonToParquet
  }
}
