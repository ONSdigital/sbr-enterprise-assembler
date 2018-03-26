package assembler


import global.{AppParams, Configs}
import service.EnterpriseAssemblerService


object AssemblerMain extends EnterpriseAssemblerService {

  def main(args: Array[String]) {
    Configs.conf.set("hbase.zookeeper.quorum", args(9))
    Configs.conf.set("hbase.zookeeper.property.clientPort", args(10))
    val appParams = args.take(9)++args.takeRight(2)
    updateLinks(AppParams(appParams))
    /*steps:
    1. load test data:
       1.1 loadFromJson
    2. Load Refresh data:
       2.1 switch to refresh set params
       2.2 loadFromJson but without(commented out) creating hfiles
       2.3 run cleanExistingRecords
       2.4 manually delete hfiles generated in prev step
       2.5 run refreshFromParquet
     * */

    //doRefresh(AppParams(appParams))
    //cleanExistingRecords(AppParams(appParams))
    //deleteFromHFile(AppParams(appParams))
    //refreshFromParquet(AppParams(appParams))
     //loadFromParquet(AppParams(appParams))
    //loadFromJson(AppParams(appParams))
    //loadFromHFile(AppParams(appParams))
    //readAll(AppParams(appParams))
    //readFromHBase(AppParams(appParams))
    //readWithFilter(AppParams(appParams))

  }

}
