package assembler


import global.Configs._
import hbase.ConnectionManagement
import service.EnterpriseAssemblerService
import spark.SparkSessionManager


object AssemblerMain extends ConnectionManagement with SparkSessionManager with EnterpriseAssemblerService {

  def main(args: Array[String]) {

     updateConf(args)
     loadFromParquet /*loadFromJson*/

   }

}