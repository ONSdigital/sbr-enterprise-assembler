package assembler


import global.{AppParams, Configs}
import service._


object AssemblerMain extends EnterpriseAssemblerService with EnterpriseRefreshService{

  def main(args: Array[String]) {
    Configs.conf.set("hbase.zookeeper.quorum", args(9))
    Configs.conf.set("hbase.zookeeper.property.clientPort", args(10))
    val appParams = args.take(9)++args.takeRight(2)
    loadRefresh(AppParams(appParams))

     //loadFromParquet(AppParams(appParams))
    //loadFromJson(AppParams(appParams))
    //loadFromHFile(AppParams(appParams))

  }

}
