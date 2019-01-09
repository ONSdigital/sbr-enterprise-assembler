import service.AddNewPeriodDataService
import util.configuration.AssemblerConfiguration._
import util.configuration.CommandLineParser

import scala.reflect.io.File

object AssemblerMain {

  def main(args: Array[String]) {

    CommandLineParser(args)

    try {

      if (createParquetFile) {
        File(PathToParquet).deleteRecursively()
        AddNewPeriodDataService.createNewPeriodParquet()
      }

      AddNewPeriodDataService.loadNewPeriodWithCalculationsData()

    } finally {

      if (isLocal) {
        File(PathToEnterpriseHFile).deleteRecursively()
        File(PathToLinksHfile).deleteRecursively()
        File(PathToLegalUnitsHFile).deleteRecursively()
        File(PathToLocalUnitsHFile).deleteRecursively()
        File(PathToReportingUnitsHFile).deleteRecursively()

        println("HFiles deleted")

      }
    }

  }

}
