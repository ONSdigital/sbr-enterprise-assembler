package closures

import dao.hbase.HFileUtils
import spark.RddLogging
import spark.calculations.AdminDataCalculator

/**
  *
  */
class CalculateClosure extends AdminDataCalculator with HFileUtils with RddLogging with Serializable{

}
