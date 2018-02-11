package assembler


import connector.HBaseConnector
import hbase.ConnectionManager
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import service.EnterpriseAssemblerService.loadFromHFile
/**
  *
  */
object AssemblerMain extends ConnectionManager
{

  def main(args: Array[String]) {
    import service.EnterpriseAssemblerService._


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
