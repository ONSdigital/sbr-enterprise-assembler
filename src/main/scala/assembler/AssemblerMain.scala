package assembler


import global.Configs.conf
import global.{AppParams, Configs}
import service._

import scala.reflect.io.File


object AssemblerMain extends CreateInitialPopulationService with EnterpriseRefreshService with AddNewPeriodDataService with DeleteDataService with DataIntegrityReportService with AdminCalculationService{

  def main(args: Array[String]) {

    conf.set("hbase.zookeeper.quorum", args(13))
    conf.set("hbase.zookeeper.property.clientPort", args(14))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 500)
    val params = args.take(13)++args.takeRight(5)

    val appParams = AppParams(params)
try{
    appParams.ACTION match{

      case "add-calculated-period" => loadNewPeriodWithCalculationsData(appParams)
      case "addperiod" => loadNewPeriodData(appParams)
      case "calculate" => addCalculations(appParams)
      case "refresh" => loadRefreshFromParquet(appParams)
      case "create" => createNewPopulationFromParquet(appParams)
      case "deleteperiod" => deletePeriod(appParams)
      case "data-integrity-report" => {
        conf.setInt("spark.sql.broadcastTimeout", 2400)
        printReport(appParams)
      }
      case arg => throw new IllegalArgumentException(s"action not recognised: $arg")

    }} finally{

        if(appParams.ENV=="local") {
          val entHFile =  File(appParams.PATH_TO_ENTERPRISE_HFILE)
          entHFile.deleteRecursively()
          val linksHFile =  File(appParams.PATH_TO_LINKS_HFILE)
          linksHFile.deleteRecursively()
          val louHFile =  File(appParams.PATH_TO_LOCALUNITS_HFILE)
          louHFile.deleteRecursively()

          println("HFiles deleted")

        }

}

  }

}
