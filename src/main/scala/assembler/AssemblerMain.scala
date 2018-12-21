package assembler


import service._
import util.options.{CommandLineParser, ConfigOptions}

import scala.reflect.io.File

object AssemblerMain extends AddNewPeriodDataService {

  def main(args: Array[String]) {

    CommandLineParser(args)

    try {
      loadNewPeriodWithCalculationsData()
    } finally {

      if (ConfigOptions.local) {
        File(ConfigOptions.PathToEnterpriseHFile).deleteRecursively()
        File(ConfigOptions.PathToLinksHfile).deleteRecursively()
        File(ConfigOptions.PathToLegalUnitsHFile).deleteRecursively()
        File(ConfigOptions.PathToLocalUnitsHFile).deleteRecursively()

        println("HFiles deleted")

      }
    }
  }

}
