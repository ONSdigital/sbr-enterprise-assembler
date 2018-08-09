package closures

import dao.hbase.HFileUtils
import global.{AppParams, Configs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import spark.extensions.sql._

trait RefreshPeriodWithCalculationsClosure extends AdminDataCalculator with BaseClosure with HFileUtils with RddLogging with Serializable{



  /**
    * Does not work currently because it's using previous period when looking up existing entities
    * and saving fresh data with new period key
    * */
  def createHFilesWithRefreshPeriodDataWithCalculations(appconf: AppParams)(implicit spark: SparkSession): Unit = {

    val allLUsDF: DataFrame = getAllLUsDF(appconf).cache()


    val allEntsDF =  getAllEntsCalculated(allLUsDF,appconf).cache()


    val allLOUs: Dataset[Row] = getAllLOUs(allEntsDF,appconf,Configs.conf).cache()


    saveLinks(allLOUs,allLUsDF,appconf)
    saveEnts(allEntsDF,appconf)
    saveLous(allLOUs,appconf)
    allLUsDF.unpersist()
    allLOUs.unpersist()
  }


  def getAllLUsDF(appconf: AppParams)(implicit spark: SparkSession) = {

    val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)

    val existingLEsDF: DataFrame = getExistingLeusDF(appconf, Configs.conf)

    //val numOfPartitions = existingLEsDF.rdd.getNumPartitions

    val joinedLUs = incomingBiDataDF.join(
      existingLEsDF.withColumnRenamed("ubrn", "id").select("id", "ern"),
      Seq("id"), "left_outer")//.repartition(numOfPartitions)

    getAllLUs(joinedLUs, appconf)

  }

  def getAllEntsCalculated(allLUsDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {

    //val numOfPartitions = allLUsDF.rdd.getNumPartitions

    val calculatedDF = calculate(allLUsDF,appconf).castAllToString
    calculatedDF.cache()


    val existingEntDF = getExistingEntsDF(appconf,Configs.conf)
    val existingEntCalculatedDF = existingEntDF.join(calculatedDF,Seq("ern"), "left_outer")//.repartition(numOfPartitions)
    val newLEUsDF = allLUsDF.join(existingEntCalculatedDF.select(col("ern")),Seq("ern"),"left_anti")//.repartition(numOfPartitions)
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"),"left_outer")//.repartition(numOfPartitions)
    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLEUsCalculatedDF).rdd,completeEntSchema)
    val allEntsDF =  existingEntCalculatedDF.union(newEntsCalculatedDF)
    calculatedDF.unpersist()
    allEntsDF
  }


  def getAllLOUs(allEntsDF:DataFrame,appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {

    val existingLOUs: DataFrame = getExistingLousDF(appconf,confs)

    val entsWithoutLOUs: DataFrame = allEntsDF.join(existingLOUs.select("ern"),Seq("ern"),"left_anti")//.repartition(numOfPartitions)

    val newAndMissingLOUsDF: DataFrame =  createNewLOUs(entsWithoutLOUs,appconf)

    existingLOUs.union(newAndMissingLOUsDF)
  }



}
object RefreshPeriodWithCalculationsClosure extends RefreshPeriodWithCalculationsClosure
