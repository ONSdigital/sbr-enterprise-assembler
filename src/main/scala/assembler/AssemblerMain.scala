package assembler


import global.Configured
import hbase.ConnectionManagement
import org.apache.hadoop.hbase.client.Connection
import service.EnterpriseAssemblerService
import spark.SparkSessionManager

import scala.util.Try
/**
  *
  */
object AssemblerMain extends ConnectionManagement with SparkSessionManager with EnterpriseAssemblerService {

  def main(args: Array[String]) {

      updateConf(args)

      withSpark{ implicit SparkSession => withHbaseConnection { implicit connection: Connection => /*loadFromParquet*/ loadFromJson}}

   }

  private def updateConf(args: Array[String]) = {
    import Configured._

    Try(args(0)).map(conf.set("hbase.table.name", _))
    Try(args(1)).map(conf.set("hbase.table.namespace", _))
    Try(args(2)).map(conf.set("files.parquet", _))
    Try(args(3)).map(conf.set("hbase.zookeeper.quorum", _))
    Try(args(4)).map(conf.set("hbase.zookeeper.property.clientPort", _))
    Try(args(5)).map(conf.set("files.hfile", _))

  }

}