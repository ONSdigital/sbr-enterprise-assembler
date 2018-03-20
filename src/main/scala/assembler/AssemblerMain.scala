package assembler


import global.{AppParams, Configs}
import service.EnterpriseAssemblerService


object AssemblerMain extends EnterpriseAssemblerService {

  def main(args: Array[String]) {
    Configs.conf.set("hbase.zookeeper.quorum", args(9))
    Configs.conf.set("hbase.zookeeper.property.clientPort", args(10))
    //Configs.conf.set(AppParams(9),AppParams(10))
     //refreshFromParquet(AppParams(args))
     loadFromParquet(AppParams(args))
     //loadFromJson(AppParams(args))
    //loadFromHFile(AppParams(args))

  }

}
