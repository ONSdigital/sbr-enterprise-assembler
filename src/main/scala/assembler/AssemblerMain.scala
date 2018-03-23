package assembler


import global.{AppParams, Configs}
import service.EnterpriseAssemblerService


object AssemblerMain extends EnterpriseAssemblerService {

  def main(args: Array[String]) {
    Configs.conf.set("hbase.zookeeper.quorum", args(9))
    Configs.conf.set("hbase.zookeeper.property.clientPort", args(10))
    val appParams = args.take(9)++args.takeRight(2)
    //deleteFromHFile(AppParams(appParams))
     //refreshFromParquet(AppParams(appParams))
     //loadFromParquet(AppParams(appParams))
     //loadFromJson(AppParams(appParams))
    loadFromHFile(AppParams(appParams))
    //readAll(AppParams(appParams))
    //readFromHBase(AppParams(appParams))
    //readWithFilter(AppParams(appParams))

  }

}
