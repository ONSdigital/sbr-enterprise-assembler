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
object AssemblerMain extends ConnectionManagement with EnterpriseAssemblerService{

  def main(args: Array[String]) {

    implicit val spark: SparkSession = init(args)

    connectionManaged{ implicit connection:Connection => loadFromParquet } //loadFromJson

    spark.stop()
  }

  private def init(args: Array[String]) = {
    import Configured._

    Try(args(0)).map(conf.set("hbase.table.name", _))
    Try(args(1)).map(conf.set("hbase.table.namespace", _))
    Try(args(2)).map(conf.set("files.parquet", _))
    Try(args(3)).map(conf.set("hbase.zookeeper.quorum", _))
    Try(args(4)).map(conf.set("hbase.zookeeper.property.clientPort", _))
    Try(args(5)).map(conf.set("files.hfile", _))

    SparkSession
      .builder()
      //.master("local[4]")
      .appName("enterprise assembler")
      .getOrCreate()

  }
}