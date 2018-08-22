package closures

import dao.hbase.HFileUtils
import global.{AppParams, Configs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import spark.extensions.sql._

trait RefreshPeriodWithCalculationsClosure extends AdminDataCalculator with BaseClosure with HFileUtils with RddLogging with Serializable{

  val newRusViewName = "NEWRUS"
  val newLeusViewName = "NEWLEUS"

  /**
    * Does not work currently because it's using previous period when looking up existing entities
    * and saving fresh data with new period key
    * */
  def createHFilesWithRefreshPeriodDataWithCalculations(appconf: AppParams)(implicit spark: SparkSession,con:Connection): Unit = {

    val allLinksLeusDF = getAllLinksLUsDF(appconf).cache()

    val allEntsDF =  getAllEntsCalculated(allLinksLeusDF,appconf).cache()

    val allRusDF = getAllRus(allEntsDF,appconf,Configs.conf).cache()

    val allLousDF = getAllLous(allRusDF,appconf,Configs.conf).cache()

    val allLeusDF = getAllLeus(allEntsDF,appconf,Configs.conf).cache()

    saveLinks(allLousDF,allRusDF,allLinksLeusDF,appconf)
    saveEnts(allEntsDF,appconf)
    saveLous(allLousDF,appconf)
    saveLeus(allLeusDF,appconf)

    allLinksLeusDF.unpersist()
    allEntsDF.unpersist()
    allLeusDF.unpersist()
    allLousDF.unpersist()
    allRusDF.unpersist()
  }


  def getAllLinksLUsDF(appconf: AppParams)(implicit spark: SparkSession) = {

    val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)

    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF(appconf, Configs.conf)

    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.withColumnRenamed("ubrn", "id").select("id", "ern"),
      Seq("id"), "left_outer")//.repartition(numOfPartitions)

    getAllLUs(joinedLUs, appconf)

  }

  def getAllEntsCalculated(allLinksLusDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {

    //val numOfPartitions = allLUsDF.rdd.getNumPartitions

    val calculatedDF = calculate(allLinksLusDF,appconf).castAllToString
    calculatedDF.cache()


    val existingEntDF = getExistingEntsDF(appconf,Configs.conf)
    val existingEntCalculatedDF = existingEntDF.join(calculatedDF,Seq("ern"), "left_outer")//.repartition(numOfPartitions)
    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")),Seq("ern"),"left_anti")//.repartition(numOfPartitions)
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"),"left_outer")//.repartition(numOfPartitions)

    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLEUsCalculatedDF,appconf).rdd,completeEntSchema)
    val newLegalUnitsDF: DataFrame = getNewLeusDF(newEntsCalculatedDF)
    newLegalUnitsDF.cache()//TODO: check if this is actually needed
    newLegalUnitsDF.createOrReplaceTempView(newLeusViewName)

    val allEntsDF =  existingEntCalculatedDF.union(newEntsCalculatedDF)
    calculatedDF.unpersist()
    allEntsDF
  }

  def getNewRusDF(newLEUsCalculatedDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {
    val newReportingUnitsDS:RDD[Row] = newLEUsCalculatedDF.rdd.map(row => new GenericRowWithSchema(Array(
      generateRurn(row,appconf),
      row.getAs[String]("ern"),
      getValueOrEmptyStr(row,"BusinessName"),
      getValueOrEmptyStr(row,"entref"),//will not be present
      getValueOrNull(row,"ruref"),//will not be present
      getValueOrNull(row,"trading_style"),//will not be present
      getValueOrEmptyStr(row,"address1"),
      getValueOrNull(row, "address2"),
      getValueOrNull(row, "address3"),
      getValueOrNull(row, "address4"),
      getValueOrNull(row, "address5"),
      getValueOrEmptyStr(row,"PostCode"),
      getValueOrEmptyStr(row,"IndustryCode"),
      getValueOrNull(row, "paye_jobs"),
      getValueOrEmptyStr(row,"employment"),
      getValueOrEmptyStr(row,"turnover"),//will not be present
      generatePrn(row,appconf)
    ),ruRowSchema))

    val newReportingUnitsDF: DataFrame = spark.createDataFrame(newReportingUnitsDS,ruRowSchema)
    newReportingUnitsDF
  }

  def getNewLeusDF(newLEUsCalculatedDF:DataFrame)(implicit spark: SparkSession) = {
    val newLegalUnitsDS:RDD[Row] = newLEUsCalculatedDF.rdd.map(row => new GenericRowWithSchema(Array(

                  row.getAs[String]("id"),
                  row.getAs[String]("ern"),
                  getValueOrNull(row,"CompanyNo"),
                  getValueOrEmptyStr(row,"BusinessName"),
                  getValueOrNull(row,"trading_style"),//will not be present
                  getValueOrEmptyStr(row,"address1"),
                  getValueOrNull(row, "address2"),
                  getValueOrNull(row, "address3"),
                  getValueOrNull(row, "address4"),
                  getValueOrNull(row, "address5"),
                  getValueOrEmptyStr(row,"PostCode"),
                  getValueOrEmptyStr(row,"IndustryCode"),
                  getValueOrNull(row, "paye_jobs"),
                  getValueOrNull(row, "Turnover"),
                  getValueOrEmptyStr(row,"LegalStatus"),
                  getValueOrNull(row, "TradingStatus"),
                  getValueOrEmptyStr(row,"birth_date"),
                  getValueOrNull(row,"death_date"),
                  getValueOrNull(row,"death_code"),
                  getValueOrNull(row,"UPRN")
                ),leuRowSchema))

    spark.createDataFrame(newLegalUnitsDS,leuRowSchema)

  }



  def getAllRus(allEntsDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingRUs: DataFrame = getExistingRusDF(appconf,confs)

    val entsWithoutRus: DataFrame = allEntsDF.join(existingRUs.select("ern"),Seq("ern"),"left_anti")

    val newAndMissingRusDF: DataFrame =  createNewRus(entsWithoutRus,appconf)

    existingRUs.union(newAndMissingRusDF)
  }

  def getAllLous(allRus:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingLous: DataFrame = getExistingLousDF(appconf,confs)

    val rusWithoutLous: DataFrame = allRus.join(existingLous.select("ern"),Seq("ern"),"left_anti")

    val newAndMissingLousDF: DataFrame =  createNewLous(rusWithoutLous,appconf)

    existingLous.union(newAndMissingLousDF)
  }



  def getAllLeus(allEntsDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingLEUs: DataFrame = getExistingLeusDF(appconf,confs)
    val tempDF = spark.sql(s"""SELECT * FROM $newLeusViewName""")
    existingLEUs.createOrReplaceTempView("EXISTINGLEUS")
    val sql =
      s"""
         SELECT * FROM EXISTINGLEUS
         UNION
         SELECT * FROM $newLeusViewName

       """.stripMargin

    spark.sql(sql)

  }

}
object RefreshPeriodWithCalculationsClosure extends RefreshPeriodWithCalculationsClosure
