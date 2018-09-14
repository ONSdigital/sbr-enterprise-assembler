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

    val allLinksLeusDF = getAllLinksLUsDF(appconf).cache()

    val allEntsDF =  getAllEntsCalculated(allLinksLeusDF,appconf).cache()

    val allRusDF = getAllRus(allEntsDF,appconf,Configs.conf).cache()

    val allLousDF = getAllLous(allRusDF,appconf,Configs.conf).cache()

    val allLeusDF = getAllLeus(allEntsDF,appconf,Configs.conf).cache()

    saveEnts(allEntsDF,appconf)
    saveRus(allRusDF,appconf)
    saveLous(allLousDF,appconf)
    saveLeus(allLeusDF,appconf)
    saveLinks(allLousDF,allRusDF,allLinksLeusDF,appconf)

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
      Seq("id"), "left_outer")

    getAllLUs(joinedLUs, appconf)

  }

  def calculateRegion(entsDF:DataFrame)(implicit spark: SparkSession) = {
    import org.apache.spark.sql.functions.udf
    import entsDF.sqlContext.implicits.StringToColumn

    def calculation = udf((postcode: String) => lookupRegionByPostcode(postcode))

    entsDF.withColumn("region",calculation($"postcode"))

  }


  def calculateWorkingProps(entsDF:DataFrame)(implicit spark: SparkSession) = {
    import org.apache.spark.sql.functions.udf
    import entsDF.sqlContext.implicits.StringToColumn

    def calculation = udf((legalStatus: String) => getWorkingPropsByLegalStatus(legalStatus))

    entsDF.withColumn("working_props",calculation($"legal_status"))

  }



  def getAllEntsCalculated(allLinksLusDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {

    val calculatedDF = calculate(allLinksLusDF,appconf).castAllToString
    calculatedDF.cache()

    val existingEntDF = getExistingEntsDF(appconf,Configs.conf)
    val existingEntsWithRegionRecalculatedDF = calculateRegion(existingEntDF)
    val existingEntsWithWorkingPropsRecalculatedDF = calculateWorkingProps(existingEntsWithRegionRecalculatedDF)
    val existingEntCalculatedDF = {
                                    val calculatedExistingRdd = existingEntsWithWorkingPropsRecalculatedDF.join(calculatedDF,Seq("ern"), "left_outer")
                                    val withDefaultValues = calculatedExistingRdd.na.fill("0", Seq("employment"))
                                    val withReorderedColumns = {
                                    val columns = completeEntSchema.fieldNames
                                    withDefaultValues.select(columns.head,columns.tail: _*)
                                    }
                                    spark.createDataFrame(withReorderedColumns.rdd, completeEntSchema)
                                  }
    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")),Seq("ern"),"left_anti")
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"),"left_outer")

    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLEUsCalculatedDF,appconf).rdd,completeEntSchema)
    val newLegalUnitsDF: DataFrame = getNewLeusDF(newLEUsCalculatedDF,appconf)
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

  def recalculateRuEmploymentAndRegion(entsDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingRUs: DataFrame = getExistingRusDF(appconf,confs)

    val ruWithEmploymentReCalculated = existingRUs.drop("employment","region").join(entsDF.select(col("ern"), col("region"), col("employment")),Seq("ern"),"inner")

    val columns: Seq[String] = ruWithEmploymentReCalculated.columns

    val reorderedColumns: Seq[String] = existingRUs.columns
   //reorder columns to comply with schema: ruRowSchema
    ruWithEmploymentReCalculated.select(reorderedColumns.head,reorderedColumns.tail: _*)
  }

  def recalculateLouEmploymentAndRegion(ruDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingLous: DataFrame = getExistingLousDF(appconf,confs)

    val lousWithEmploymentReCalculated = existingLous.drop("employment","region").join(ruDF.select(col("rurn"), col("region"), col("employment")),Seq("rurn"),"inner")

    val columnsOrdered = existingLous.columns

    lousWithEmploymentReCalculated.select(columnsOrdered.head,columnsOrdered.tail: _*)

  }


  def getAllRus(allEntsDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingRUs: DataFrame = recalculateRuEmploymentAndRegion(allEntsDF,appconf,confs)

    val entsWithoutRus: DataFrame = allEntsDF.join(existingRUs.select("ern"),Seq("ern"),"left_anti")

    val newAndMissingRusDF: DataFrame = createNewRus(entsWithoutRus,appconf)

    existingRUs.union(newAndMissingRusDF)
  }

  def getAllLous(allRus:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingLous: DataFrame = recalculateLouEmploymentAndRegion(allRus,appconf,confs)

    val rusWithoutLous: DataFrame = allRus.join(existingLous.select("rurn"),Seq("rurn"),"left_anti")

    val newAndMissingLousDF: DataFrame =  createNewLous(rusWithoutLous,appconf)

    existingLous.union(newAndMissingLousDF)
  }



  def getAllLeus(allEntsDF:DataFrame, appconf: AppParams, confs:Configuration)(implicit spark: SparkSession) = {

    val existingLEUs: DataFrame = getExistingLeusDF(appconf,confs)
    println("EXISTING LEU START******************************************")
    existingLEUs.show(100)
    println("EXISTING LEU END********************************************")
    val newLeusDF = spark.sql(s"""SELECT * FROM $newLeusViewName""")
    existingLEUs.union(newLeusDF)
  }

}
object RefreshPeriodWithCalculationsClosure extends RefreshPeriodWithCalculationsClosure
