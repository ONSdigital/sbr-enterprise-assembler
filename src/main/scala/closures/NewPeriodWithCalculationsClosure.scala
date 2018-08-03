package closures

import dao.hbase.HFileUtils
import global.{AppParams, Configs}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import spark.extensions.sql._

import scala.util.Try

trait NewPeriodWithCalculationsClosure extends AdminDataCalculator with BaseClosure with HFileUtils with RddLogging with Serializable{




  def addNewPeriodDataWithCalculations(appconf: AppParams)(implicit spark: SparkSession): Unit = {

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
    val newEntsCalculatedDF = spark.createDataFrame(createNewEnts(newLEUsCalculatedDF).rdd,completeEntSchema)
    val allEntsDF =  existingEntCalculatedDF.union(newEntsCalculatedDF)
    calculatedDF.unpersist()
    allEntsDF
  }


  def getAllLOUs(allEntsDF:DataFrame,appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    //val numOfPartitions = allEntsDF.rdd.getNumPartitions

    val existingLOUs: DataFrame = getExistingLousDF(appconf,confs)

    val entsWithoutLOUs: DataFrame = allEntsDF.join(existingLOUs.select("ern"),Seq("ern"),"left_anti")//.repartition(numOfPartitions)

    val newAndMissingLOUsDF: DataFrame =  createNewAndMissingLOUs(entsWithoutLOUs,appconf)

    existingLOUs.union(newAndMissingLOUsDF)
  }

  def createNewAndMissingLOUs(ents: DataFrame, appconf: AppParams)(implicit spark: SparkSession) = {

    spark.createDataFrame(
      ents.rdd.map(row => Row(
        generateLurn(row,appconf),
        Try {row.getAs[String]("luref")}.getOrElse(null),
        row.getAs[String]("ern"),
        row.getAs[String]("name"),
        Try {row.getAs[String]("entref")}.getOrElse(null),
        Try {row.getAs[String]("trading_style")}.getOrElse(null),
        row.getAs[String]("address1"),
        Try {row.getAs[String]("address2")}.getOrElse(null),
        Try {row.getAs[String]("address3")}.getOrElse (null),
        Try {row.getAs[String]("address4")}.getOrElse (null),
        Try {row.getAs[String]("address5")}.getOrElse (null),
        getValueOrEmptyStr(row,"postcode"),
        getValueOrEmptyStr(row,"sic07"),
        getValueOrEmptyStr(row,"paye_empees")
      )), louRowSchema)
  }
  /**
    * Creates new Enterprises from new LEUs with calculations
    * schema: completeNewEntSchema
    * ern, entref, name, address1, postcode, sic07, legal_status,
    * AND calculations:
    * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
    * */
  //paye_empees|paye_jobs|cntd_turnover|app_turnover|std_turnover|grp_turnover|ent_turnover
  def createNewEnts(newLEUsCalculatedDF:DataFrame)(implicit spark: SparkSession) = spark.createDataFrame(
    newLEUsCalculatedDF.rdd.map(row => Row(
      row.getAs[String]("ern"),
      Try {row.getAs[String]("entref")}.getOrElse(null),
      row.getAs[String]("BusinessName"),
      null, //trading_style
      Try {row.getAs[String]("address1")}.getOrElse(""),
      null, null, null, null, //address2,3,4,5
      row.getAs[String]("PostCode"),
      Try {row.getAs[String]("IndustryCode")}.getOrElse(""),
      row.getAs[String]("LegalStatus"),
      Try {row.getAs[String]("paye_empees")}.getOrElse(null),
      Try {row.getAs[String]("paye_jobs")}.getOrElse(null),
      Try {row.getAs[String]("cntd_turnover")}.getOrElse(null),
      Try {row.getAs[String]("app_turnover")}.getOrElse(null),
      Try {row.getAs[String]("std_turnover")}.getOrElse(null),
      Try {row.getAs[String]("grp_turnover")}.getOrElse(null),
      Try {row.getAs[String]("ent_turnover")}.getOrElse(null)
    )
    ), completeEntSchema)
/**
  * Fields:
  * id,BusinessName, UPRN, PostCode,IndustryCode,LegalStatus,
  * TradingStatus, Turnover, EmploymentBands, PayeRefs, VatRefs, CompanyNo
  * */
  def getIncomingBiData(appconf: AppParams)(implicit spark: SparkSession) = {
    val updatedConfs = appconf.copy(TIME_PERIOD = appconf.PREVIOUS_TIME_PERIOD)
    val parquetDF = spark.read.parquet(appconf.PATH_TO_PARQUET)
    parquetDF.castAllToString
  }

  def getAllLUs(joinedLUs: DataFrame,appconf:AppParams)(implicit spark: SparkSession) = {

    val rows = joinedLUs.rdd.map { row => {
      val ern = if (row.isNull("ern")) generateErn(row, appconf) else row.getAs[String]("ern")

      Row(
        ern,
        row.getAs[String]("id"),
        row.getAs[String]("BusinessName"),
        row.getAs[String]("IndustryCode"),
        row.getAs[String]("LegalStatus"),
        getValueOrEmptyStr(row,"PostCode"),
        row.getAs[String]("TradingStatus"),
        row.getAs[String]("Turnover"),
        row.getAs[String]("UPRN"),
        row.getAs[String]("CompanyNo"),
        row.getAs[Seq[String]]("PayeRefs"),
        row.getAs[Seq[String]]("VatRefs")
      )}}
    spark.createDataFrame(rows, biWithErnSchema)
  }

}
object NewPeriodWithCalculationsClosure extends NewPeriodWithCalculationsClosure
