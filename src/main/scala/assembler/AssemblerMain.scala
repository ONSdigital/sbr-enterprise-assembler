package assembler


import global.Configured
import hbase.ConnectionManagement
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import service.EnterpriseAssemblerService

import scala.util.Try
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

      //loadFromJson

      Try{(args(0),args(1),args(2))}.map{args =>

        val (tableName, nameSpace, pathToParquet) = args
        loadFromParquet(tableName, nameSpace, pathToParquet)
      }.getOrElse (loadFromJson)

      spark.stop()
  }
 }
}
