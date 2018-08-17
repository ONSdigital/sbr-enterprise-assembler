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
    allEntsDF.unpersist()
    allLEUs.unpersist()
    allLOUs.unpersist()
  }


  def getAllLinksLUsDF(appconf: AppParams)(implicit spark: SparkSession) = {

    val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)

    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF(appconf, Configs.conf)

    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.withColumnRenamed("ubrn", "id").select("id", "ern"),
      Seq("id"), "left_outer")//.repartition(numOfPartitions)

     getAllLUs(joinedLUs, appconf)

  }

  def getAllEntsCalculated(allLinksLusDF:DataFrame,appconf: AppParams,newLeusViewName:String = "NEWLEUS")(implicit spark: SparkSession) = {

    //val numOfPartitions = allLUsDF.rdd.getNumPartitions

    val calculatedDF = calculate(allLinksLusDF,appconf).castAllToString
    calculatedDF.cache()


    val existingEntDF = getExistingEntsDF(appconf,Configs.conf)
    val existingEntCalculatedDF = existingEntDF.join(calculatedDF,Seq("ern"), "left_outer")//.repartition(numOfPartitions)
    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")),Seq("ern"),"left_anti")//.repartition(numOfPartitions)
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"),"left_outer")//.repartition(numOfPartitions)

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

    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLEUsCalculatedDF,appconf).rdd,completeEntSchema)
    val newLegalUnitsDF: DataFrame = spark.createDataFrame(newLegalUnitsDS,leuRowSchema)
    newLegalUnitsDF.cache()
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
    val tempDF = spark.sql("""SELECT * FROM NEWLEUS""")
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
