package assembler


import global.Configs._
import service.EnterpriseAssemblerService


object AssemblerMain extends EnterpriseAssemblerService {

  def main(args: Array[String]) {

     updateConf(args)
     //refreshFromParquet
     loadFromParquet
    //loadFromJson
    //loadFromHFile

  }

}