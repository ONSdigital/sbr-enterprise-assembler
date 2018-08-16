package closures

import dao.hbase.HFileUtils
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql._
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import spark.extensions.sql._

/**
  *
  */
trait CreateClosures extends AdminDataCalculator with BaseClosure with HFileUtils with RddLogging with Serializable{


  def parquetCreateNewToHFile(implicit spark:SparkSession, con:Connection, appconf:AppParams){

    val appArgs = appconf

    val payeDF = spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)
    val vatDF  = spark.read.option("header", "true").csv(appconf.PATH_TO_VAT)

    val stringifiedParquet = spark.read.parquet(appArgs.PATH_TO_PARQUET).castAllToString
    val newLEUsCalculatedDF = calculate(stringifiedParquet,appconf).castAllToString
    newLEUsCalculatedDF.cache()
    val allLUsDF = getAllLUs(newLEUsCalculatedDF,appconf)

    val allEntsDF = spark.createDataFrame(createNewEnts(allLUsDF,appArgs).rdd,completeEntSchema).cache()

    val allLOUs: Dataset[Row] = createNewLOUs(allEntsDF,appconf).cache()

    saveLinks(allLOUs,allLUsDF,appconf)
    saveEnts(allEntsDF,appconf)
    saveLous(allLOUs,appconf)
    allLUsDF.unpersist()
    allLOUs.unpersist()
  }

}

object CreateClosures extends CreateClosures