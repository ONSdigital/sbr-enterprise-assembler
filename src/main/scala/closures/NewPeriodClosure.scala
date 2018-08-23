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
import spark.extensions.sql._

import scala.util.Try

/**
  *No calculations added
  */
class NewPeriodClosure extends HFileUtils with BaseClosure with RddLogging with Serializable {

  val newRusViewName = "NEWRUS"
  val newLeusViewName = "NEWLEUS"

  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession,con:Connection): Unit = {



      val allLinksLeusDF = getAllLinksLUsDF(appconf).cache()

      val allEntsDF =  getAllEnts(allLinksLeusDF,appconf).cache()

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

    def getAllEnts(allLinksLusDF:DataFrame, appconf: AppParams)(implicit spark: SparkSession) = {
      val existingEntDF = getExistingEntsDF(appconf,Configs.conf)
      val newLEUsDF = allLinksLusDF.join(existingEntDF.select(col("ern")),Seq("ern"),"left_anti")
      val newLegalUnitsDF: DataFrame = getNewLusDF(newLEUsDF)
      val newEntsDF = spark.createDataFrame(createNewEnts(newLegalUnitsDF,appconf).rdd,entRowSchema)
      newLegalUnitsDF.cache()//TODO: check if this is actually needed
      newLegalUnitsDF.createOrReplaceTempView(newLeusViewName)
      val allEntsDF =  existingEntDF.union(newEntsDF)
      allEntsDF
    }

    def getNewRusDF(newLEUsCalculatedDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {
      val newReportingUnitsDS:RDD[Row] = newLEUsCalculatedDF.rdd.map(row => new GenericRowWithSchema(Array(
        generateRurn(row,appconf),
        row.getAs[String]("ern"),
        row.getValueOrEmptyStr("BusinessName"),
        row.getValueOrEmptyStr("entref"),//will not be present
        row.getValueOrNull("ruref"),//will not be present
        row.getValueOrNull("trading_style"),//will not be present
        row.getValueOrEmptyStr("address1"),
        row.getValueOrNull( "address2"),
        row.getValueOrNull( "address3"),
        row.getValueOrNull( "address4"),
        row.getValueOrNull( "address5"),
        row.getValueOrEmptyStr("PostCode"),
        row.getValueOrEmptyStr("IndustryCode"),
        row.getValueOrNull( "paye_jobs"),
        row.getValueOrEmptyStr("employment"),
        row.getValueOrEmptyStr("turnover"),//will not be present
        generatePrn(row,appconf)
      ),ruRowSchema))

      val newReportingUnitsDF: DataFrame = spark.createDataFrame(newReportingUnitsDS,ruRowSchema)
      newReportingUnitsDF
    }

    def getNewLusDF(newLEUsCalculatedDF:DataFrame)(implicit spark: SparkSession) = {
      val newLegalUnitsDS:RDD[Row] = newLEUsCalculatedDF.rdd.map(row => new GenericRowWithSchema(Array(

        row.getAs[String]("id"),
        row.getAs[String]("ern"),
        row.getValueOrNull("CompanyNo"),
        row.getValueOrEmptyStr("BusinessName"),
        row.getValueOrNull("trading_style"),//will not be present
        row.getValueOrEmptyStr("address1"),
        row.getValueOrNull( "address2"),
        row.getValueOrNull( "address3"),
        row.getValueOrNull( "address4"),
        row.getValueOrNull( "address5"),
        row.getValueOrEmptyStr("PostCode"),
        row.getValueOrEmptyStr("IndustryCode"),
        row.getValueOrNull( "paye_jobs"),
        row.getValueOrNull( "Turnover"),
        row.getValueOrEmptyStr("LegalStatus"),
        row.getValueOrNull( "TradingStatus"),
        row.getValueOrEmptyStr("birth_date"),
        row.getValueOrNull("death_date"),
        row.getValueOrNull("death_code"),
        row.getValueOrNull("UPRN")
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

object NewPeriodClosure extends NewPeriodClosure