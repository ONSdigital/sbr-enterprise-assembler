package assembler


import global.ApplicationConfig
import hbase.ConnectionManager
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import service.EnterpriseAssemblerService
/**
  *
  */
object AssemblerMain extends ApplicationConfig with ConnectionManager with EnterpriseAssemblerService{

  def main(args: Array[String]) {


    connectionManaged{ implicit connection:Connection => {

      implicit val spark: SparkSession = SparkSession
        .builder()
        .master("local[4]")
        .appName("enterprise assembler")
        .getOrCreate()

      //loadFromJson
      //loadFromParquet
      loadFromHFile

      spark.stop()
  }}
 }
}
