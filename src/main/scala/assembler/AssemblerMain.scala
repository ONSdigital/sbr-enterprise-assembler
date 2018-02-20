package assembler


import global.Configured
import hbase.ConnectionManagement
import service.EnterpriseAssemblerService
import spark.SparkSessionManager
import scala.util.Try



object AssemblerMain extends ConnectionManagement with SparkSessionManager with EnterpriseAssemblerService {

  def main(args: Array[String]) {

      updateConf(args)
     loadFromParquet /*loadFromJson*/

   }

  private def updateConf(args: Array[String]) = {
    import Configured._

    Try(args(0)).map(conf.set("hbase.table.links.name", _)).getOrElse(Unit)
    Try(args(1)).map(conf.set("hbase.table.links.namespace", _)).getOrElse(Unit)
    Try(args(2)).map(conf.set("files.links.hfile", _)).getOrElse(Unit)
    Try(args(3)).map(conf.set("hbase.table.enterprise.name", _)).getOrElse(Unit)
    Try(args(4)).map(conf.set("hbase.table.enterprise.namespace", _)).getOrElse(Unit)
    Try(args(5)).map(conf.set("files.enterprise.hfile", _)).getOrElse(Unit)
    Try(args(6)).map(conf.set("files.parquet", _)).getOrElse(Unit)
    Try(args(7)).map(conf.set("hbase.zookeeper.quorum", _)).getOrElse(Unit)
    Try(args(8)).map(conf.set("hbase.zookeeper.property.clientPort", _)).getOrElse(Unit)

  }

}