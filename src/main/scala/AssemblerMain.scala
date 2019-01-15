import org.apache.log4j.{Level, LogManager, Logger}
import service.AddNewPeriodDataService
import util.configuration.AssemblerConfiguration._
import util.configuration.CommandLineParser

import scala.reflect.io.File

object AssemblerMain {

  def main(args: Array[String]) {

    /**
      * Use a custom logging level so we can display log messages independently, e.g. in log4j.properties :
      *   log4j.EnterpriseAssembler=INFO, console
      */

    @transient lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")
    LogManager.getLogger("EnterpriseAssembler").setLevel(Level.INFO)

    log.info("Starting enterprise assembler")

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

      }
    }

    log.info("enterprise assembler completed")
  }

}
