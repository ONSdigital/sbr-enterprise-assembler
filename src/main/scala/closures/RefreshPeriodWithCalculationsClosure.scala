package closures

import global.{AppParams, Configs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, _}
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import spark.extensions.sql._

trait RefreshPeriodWithCalculationsClosure extends AdminDataCalculator with BaseClosure with RddLogging with Serializable{

  val newRusViewName = "NEWRUS"
  val newLeusViewName = "NEWLEUS"

  /**
    * Does not work currently because it's using previous period when looking up existing entities
    * and saving fresh data with new period key
    * */
  override def createUnitsHfiles(appconf: AppParams)(implicit spark: SparkSession, con:Connection): Unit = {

    val regionsByPostcodeDF = spark.read.option("header", "true").csv(appconf.PATH_TO_GEO).select("pcds","rgn").toDF("postcode", "region").cache()

    val allLinksLeusDF = getAllLinksLUsDF(appconf).cache()

    val allEntsDF =  getAllEntsCalculated(allLinksLeusDF,regionsByPostcodeDF,appconf).cache()

    val allRusDF = getAllRus(allEntsDF,regionsByPostcodeDF,appconf,Configs.conf).cache()

    val allLousDF = getAllLous(allRusDF,regionsByPostcodeDF,appconf,Configs.conf).cache()

    val allLeusDF = getAllLeus(appconf,Configs.conf).cache()

    saveEnts(allEntsDF,appconf)
    saveRus(allRusDF,appconf)
    saveLous(allLousDF,appconf)
    saveLeus(allLeusDF,appconf)
    saveLinks(allLousDF,allRusDF,allLinksLeusDF,appconf)

    allLeusDF.unpersist()
    allLousDF.unpersist()
    allRusDF.unpersist()
    allEntsDF.unpersist()
    allLinksLeusDF.unpersist()
    regionsByPostcodeDF.unpersist()
  }


  def getAllLinksLUsDF(appconf: AppParams)(implicit spark: SparkSession) = {

    val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)

    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF(appconf, Configs.conf)

    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.withColumnRenamed("ubrn", "id").select("id", "ern"),
      Seq("id"), "left_outer")

    getAllLUs(joinedLUs, appconf)

  }


  def getAllEntsCalculated(allLinksLusDF:DataFrame,regionsByPostcodeDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {

    val calculatedDF = calculate(allLinksLusDF,appconf).castAllToString
    calculatedDF.cache()

    val existingEntDF = getExistingEntsDF(appconf,Configs.conf)


    val existingEntCalculatedDF = {
                                    val calculatedExistingRdd = existingEntDF.join(calculatedDF,Seq("ern"), "left_outer")
                                    val existingEntsWithAllDynamicValuesRecalculatedDF = calculateDynamicValues(calculatedExistingRdd, regionsByPostcodeDF)
                                    val withReorderedColumns = {
                                         val columns = completeEntSchema.fieldNames
                                         existingEntsWithAllDynamicValuesRecalculatedDF.select( columns.head, columns.tail: _*)
                                    }
                                    spark.createDataFrame(withReorderedColumns.rdd, completeEntSchema)
                                  }
    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")),Seq("ern"),"left_anti")
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"),"left_outer")

    val newLeusWithWorkingPropsAndRegionDF = calculateDynamicValues(newLEUsCalculatedDF.withColumnRenamed("LegalStatus","legal_status").withColumnRenamed("PostCode","postcode"),regionsByPostcodeDF)

    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLeusWithWorkingPropsAndRegionDF,appconf).rdd,completeEntSchema)
    val newLegalUnitsDF: DataFrame = getNewLeusDF(newLeusWithWorkingPropsAndRegionDF,appconf)
    newLegalUnitsDF.cache()//TODO: check if this is actually needed
    newLegalUnitsDF.createOrReplaceTempView(newLeusViewName)

    val allEntsDF =  existingEntCalculatedDF.union(newEntsCalculatedDF)
    calculatedDF.unpersist()
    allEntsDF
  }


  def getNewLeusDF(newLEUsCalculatedDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {
    val newLegalUnitsDS:RDD[Row] = newLEUsCalculatedDF.rdd.map(row => new GenericRowWithSchema(Array(

                  row.getAs[String]("id"),
                  row.getAs[String]("ern"),
                  generatePrn(row,appconf),
                  row.getValueOrNull("CompanyNo"),
                  row.getValueOrEmptyStr("BusinessName"),
                  row.getValueOrNull("trading_style"),//will not be present
                  row.getValueOrEmptyStr("address1"),
                  row.getValueOrNull( "address2"),
                  row.getValueOrNull( "address3"),
                  row.getValueOrNull( "address4"),
                  row.getValueOrNull( "address5"),
                  row.getValueOrEmptyStr("postcode"),
                  row.getValueOrEmptyStr("IndustryCode"),
                  row.getValueOrNull( "paye_jobs"),
                  row.getValueOrNull( "Turnover"),
                  row.getValueOrEmptyStr("legal_status"),
                  row.getValueOrNull( "TradingStatus"),
                  row.getValueOrEmptyStr("birth_date"),
                  row.getValueOrNull("death_date"),
                  row.getValueOrNull("death_code"),
                  row.getValueOrNull("UPRN")
                ),leuRowSchema))

    spark.createDataFrame(newLegalUnitsDS,leuRowSchema)

  }


  def recalculateLouEmploymentAndRegion(ruDF:DataFrame, regionsByPostcodeDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingLous: DataFrame = getExistingLousDF(appconf,confs)

    val lousWithEmploymentReCalculated = existingLous.drop("employment").join(ruDF.select(col("rurn"), col("employment")),Seq("rurn"),"inner")

    val lousWithRegionRecalcuated = calculateRegion(lousWithEmploymentReCalculated,regionsByPostcodeDF)

    lousWithRegionRecalcuated

  }


  def getAllRus(allEntsDF:DataFrame, regionsByPostcodeDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingRUs: DataFrame = getExistingRusDF(appconf,confs)

    //val existingRusWithRecalculatedEmployment =
    val columns = ruRowSchema.fieldNames
    val ruWithRegion: DataFrame = calculateRegion(existingRUs,regionsByPostcodeDF).select(columns.head, columns.tail: _*)

    val entsWithoutRus: DataFrame = allEntsDF.join(ruWithRegion.select("ern"),Seq("ern"),"left_anti")

    val newAndMissingRusDF: DataFrame = createNewRus(entsWithoutRus,appconf).select(columns.head, columns.tail: _*)

    val res = ruWithRegion.union(newAndMissingRusDF)

    res
  }

  def getAllLous(allRus:DataFrame, regionsByPostcodeDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val columns = louRowSchema.fieldNames

    val existingLous: DataFrame = getExistingLousDF(appconf,confs)

    val existingLousWithRegion: DataFrame = calculateRegion(existingLous,regionsByPostcodeDF).select(columns.head, columns.tail: _*)

    val rusWithoutLous: DataFrame = allRus.join(existingLousWithRegion.select("rurn"),Seq("rurn"),"left_anti")

    val newAndMissingLousDF: DataFrame =  createNewLous(rusWithoutLous,appconf)

    existingLousWithRegion.union(newAndMissingLousDF)
  }



  def getAllLeus(appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {
                                          val existingLEUs: DataFrame = getExistingLeusDF(appconf,confs)
                                          val newLeusDF = spark.sql(s"""SELECT * FROM $newLeusViewName""")
                                          existingLEUs.union(newLeusDF)
                                       }

}
object RefreshPeriodWithCalculationsClosure extends RefreshPeriodWithCalculationsClosure
