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



  /**
    * Does not work currently because it's using previous period when looking up existing entities
    * and saving fresh data with new period key
    * */
  def createHFilesWithRefreshPeriodDataWithCalculations(appconf: AppParams)(implicit spark: SparkSession,con:Connection): Unit = {

    val allLinksLusDF: DataFrame = getAllLinksLUsDF(appconf).cache()


    val allEntsDF =  getAllEntsCalculated(allLinksLusDF,appconf).cache()


    val allLOUs: DataFrame = getAllLOUs(allEntsDF,appconf,Configs.conf).cache()


    val allLEUs: Dataset[Row] = getAllLEUs(allEntsDF,appconf,Configs.conf).cache()

    saveLinks(allLOUs,allLinksLusDF,appconf)
    saveEnts(allEntsDF,appconf)
    saveLous(allLOUs,appconf)
    saveLeus(allLEUs,appconf)
    allLinksLusDF.unpersist()
    allLOUs.unpersist()
  }


  def getAllLinksLUsDF(appconf: AppParams)(implicit spark: SparkSession) = {

    val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)

    val existingLEsDF: DataFrame = getExistingLinksLeusDF(appconf, Configs.conf)

    //val numOfPartitions = existingLEsDF.rdd.getNumPartitions

    val joinedLUs = incomingBiDataDF.join(
      existingLEsDF.withColumnRenamed("ubrn", "id").select("id", "ern"),
      Seq("id"), "left_outer")//.repartition(numOfPartitions)

     getAllLUs(joinedLUs, appconf)

  }

  def getAllEntsCalculated(allLUsDF:DataFrame,appconf: AppParams,newLeusViewName:String = "NEWLEUS")(implicit spark: SparkSession) = {

    //val numOfPartitions = allLUsDF.rdd.getNumPartitions

    val calculatedDF = calculate(allLUsDF,appconf).castAllToString
    calculatedDF.cache()


    val existingEntDF = getExistingEntsDF(appconf,Configs.conf)
    val existingEntCalculatedDF = existingEntDF.join(calculatedDF,Seq("ern"), "left_outer")//.repartition(numOfPartitions)
    val newLEUsDF = allLUsDF.join(existingEntCalculatedDF.select(col("ern")),Seq("ern"),"left_anti")//.repartition(numOfPartitions)
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"),"left_outer")//.repartition(numOfPartitions)
    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLEUsCalculatedDF,appconf).rdd,completeEntSchema)
    val newLegalUnitsDS = newLEUsCalculatedDF.rdd.map(row => Row(

        row.getAs[String]("id"),
        row.getAs[String]("ern"),
        row.getAs[String]("CompanyNo"),
        getValueOrEmptyStr(row,"BusinessName"),
        row.getAs[String]("trading_style"),//will not be present
        getValueOrEmptyStr(row,"address1"),
        row.getAs[String]("address2"),
        row.getAs[String]("address3"),
        row.getAs[String]("address4"),
        row.getAs[String]("address5"),
        getValueOrEmptyStr(row,"PostCode"),
        getValueOrEmptyStr(row,"IndustryCode"),
        row.getAs[String]("paye_jobs"),
        row.getAs[String]("Turnover"),
        getValueOrEmptyStr(row,"LegalStatus"),
        row.getAs[String]("TradingStatus"),
        getValueOrEmptyStr(row,"birth_date"),
        row.getAs[String]("death_date"),
        row.getAs[String]("death_code"),
        row.getAs[String]("UPRN")
      ))

    val newLegalUnitsDF: DataFrame = spark.createDataFrame(newLegalUnitsDS,leuRowSchema)
    newLegalUnitsDF.createOrReplaceTempView(newLeusViewName)
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

  def getAllLEUs(allEntsDF:DataFrame,appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {

    val existingLEUs: DataFrame = getExistingLeusDF(appconf,confs)
    existingLEUs.createOrReplaceTempView("EXISTINGLEUS")
    val sql =
      s"""
         SELECT * FROM EXISTINGLEUS
         UNION
         SELECT * FROM NEWLEUS

       """.stripMargin

    spark.sql(sql)

  }

}
object RefreshPeriodWithCalculationsClosure extends RefreshPeriodWithCalculationsClosure
