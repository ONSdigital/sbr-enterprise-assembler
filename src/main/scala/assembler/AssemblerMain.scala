package assembler


import global.Configured
import hbase.ConnectionManagement
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import service.EnterpriseAssemblerService
/**
  *
  */
object AssemblerMain extends Configured with ConnectionManagement with EnterpriseAssemblerService{

  def main(args: Array[String]) {


    connectionManaged{ implicit connection:Connection =>

      implicit val spark: SparkSession = SparkSession
        .builder()
        .master("local[4]")
        .appName("enterprise assembler")
        .getOrCreate()

      if (args.nonEmpty) loadFromJson(args(0)) else loadFromJson
      //if (args.nonEmpty) loadFromParquet(args(0)) else loadFromParquet
      //loadFromHFile

      spark.stop()
  }
 }
}
