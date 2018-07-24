package closures

import dao.hbase.HFileUtils
import model.domain.HFileRow
import org.apache.spark.sql.SparkSession
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import spark.extensions.rdd.HBaseDataReader

/**
  *
  */
class CalculateClosure extends AdminDataCalculator with  RddLogging with Serializable{
  def readLinksHFile(conf:global.AppParams)(implicit spark:SparkSession) = {
    val rows = HBaseDataReader.readEntitiesFromHFile[HFileRow](conf.PATH_TO_LINKS_HFILE)
    printRdd("rows",rows,"HFileRows")
  }
}
