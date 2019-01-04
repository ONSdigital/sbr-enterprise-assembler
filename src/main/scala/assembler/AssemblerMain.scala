package assembler


import java.nio.file.Path

import service._
import util.options.{CommandLineParser, ConfigOptions}

import scala.reflect.io.File

object AssemblerMain extends AddNewPeriodDataService {

  def main(args: Array[String]) {

    CommandLineParser(args)

    try {

      if (ConfigOptions.CreateParquet == "true") {
        File(ConfigOptions.PathToParquet).deleteRecursively()
        createNewPeriodParquet()
      }

      loadNewPeriodWithCalculationsData()

    } finally {

      if (ConfigOptions.local) {
        File(ConfigOptions.PathToEnterpriseHFile).deleteRecursively()
        File(ConfigOptions.PathToLinksHfile).deleteRecursively()
        File(ConfigOptions.PathToLegalUnitsHFile).deleteRecursively()
        File(ConfigOptions.PathToLocalUnitsHFile).deleteRecursively()
        File(ConfigOptions.PathToReportingUnitsHFile).deleteRecursively()

        println("HFiles deleted")

      }
    }

  }

}
