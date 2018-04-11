package assembler


import global.{AppParams, Configs}
import service._


object AssemblerMain extends EnterpriseAssemblerService with EnterpriseRefreshService{

  def main(args: Array[String]) {
    Configs.conf.set("hbase.zookeeper.quorum", args(9))
    Configs.conf.set("hbase.zookeeper.property.clientPort", args(10))
    val appParams = args.take(9)++args.takeRight(2)
    //createRefreshParquet(AppParams(appParams))
    //loadRefreshFromHFiles(AppParams(appParams))
    loadRefresh(AppParams(appParams))
    /*
    3recsRefresh:
    1. where "id": 21840175, "VatRefs": [10000, 20000]
    2. where "id": 28919372, "PayeRefs": ["20002", "30003"]

    AFTER REFRESH:
    1. "VatRefs": [10000, 20000, 30000]
    2.  "PayeRefs": ["30003"]

    as result refreshed enterprise has employment calculations:
     change from 8 - 10 && 2 - 4 change to 8-10 and 0 - 0
    */

    loadFromParquet(AppParams(appParams))
     //loadFromJson(AppParams(appParams))
    //loadFromHFile(AppParams(appParams))

  }

}
