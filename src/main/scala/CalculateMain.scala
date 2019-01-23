import org.apache.log4j.{Level, LogManager, Logger}
import service.calculations.Calculate
import util.configuration.AssemblerConfiguration._
import util.configuration.CommandLineParser

import scala.reflect.io.File

object CalculateMain {
  def main(args: Array[String]) {

    /**
      * Use a custom logging level so we can display log messages independently, e.g. in log4j.properties :
      * log4j.EnterpriseAssembler=INFO, console
      */

    @transient lazy val log: Logger = Logger.getLogger("CalcOfUnits")
    LogManager.getLogger("CalcOfUnits").setLevel(Level.INFO)

    log.info("Starting Units Calculations")

    CommandLineParser(args)


    Calculate.hfileAndLoad


    log.info("Units Calculations completed")
  }


}
