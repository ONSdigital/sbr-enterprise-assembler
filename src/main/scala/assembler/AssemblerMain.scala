package assembler


import global.AppParams
import service.EnterpriseAssemblerService


object AssemblerMain extends EnterpriseAssemblerService {

  def main(args: Array[String]) {
     //refreshFromParquet(AppParams(args))
     //loadFromParquet(AppParams(args))
     loadFromJson(AppParams(args))
    //loadFromHFile(AppParams(args))

  }

}