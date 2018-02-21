package assembler


import dao.hbase.HBaseConnectionManager
import global.Configs._
import service.EnterpriseAssemblerService
import spark.SparkSessionManager


object AssemblerMain extends HBaseConnectionManager with SparkSessionManager with EnterpriseAssemblerService {

  def main(args: Array[String]) {

     updateConf(args)
     loadFromParquet /*loadFromJson*/

   }

}