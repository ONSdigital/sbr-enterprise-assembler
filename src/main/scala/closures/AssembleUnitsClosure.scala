package closures

import util.options._
import dao.hive.HiveDao
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import spark.calculations.SmlAdminDataCalculator
import spark.extensions.sql._

trait AssembleUnitsClosure extends SmlAdminDataCalculator with BaseClosure with Serializable {

  val newRusViewName = "NEWRUS"
  val newLeusViewName = "NEWLEUS"

  /**
    * Does not work currently because it's using previous period when looking up existing entities
    * and saving fresh data with new period key
    **/
  override def createUnitsHfiles(implicit spark: SparkSession, con: Connection): Unit = {

    val regionsByPostcodeDF: DataFrame = if (ConfigOptions.local) {
      spark.read.option("header", "true").csv(ConfigOptions.PathToGeo).select("pcds", "rgn").toDF("postcode", "region").cache()
    } else {
      HiveDao.getRegions().cache()
    }

    val regionsByPostcodeShortDF: DataFrame = if (ConfigOptions.local) {
      spark.read.option("header", "true").csv(ConfigOptions.PathToGeoShort).select("pcds", "rgn").toDF("postcodeout", "region").cache()
    } else {
      HiveDao.getRegionsShort()
    }

    regionsByPostcodeDF.collect()

    val allLinksLeusDF = getAllLinksLUsDF().cache()

    val allEntsDF = getAllEntsCalculated(allLinksLeusDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache()

    val allRusDF = getAllRus(allEntsDF, regionsByPostcodeDF, regionsByPostcodeShortDF, ConfigOptions.hbaseConfiguration).cache()

    val allLousDF = getAllLous(allRusDF, regionsByPostcodeDF, regionsByPostcodeShortDF, ConfigOptions.hbaseConfiguration).cache()

    val allLeusDF = getAllLeus(ConfigOptions.hbaseConfiguration).cache()

    saveEnts(allEntsDF)
    saveRus(allRusDF)
    saveLous(allLousDF)
    saveLeus(allLeusDF)
    saveLinks(allLousDF, allRusDF, allLinksLeusDF)

    allLeusDF.unpersist()
    allLousDF.unpersist()
    allRusDF.unpersist()
    allEntsDF.unpersist()
    allLinksLeusDF.unpersist()
    regionsByPostcodeDF.unpersist()
  }

  def getAllLinksLUsDF()(implicit spark: SparkSession): DataFrame = {

    val incomingBiDataDF: DataFrame = getIncomingBiData()

    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF(ConfigOptions.hbaseConfiguration)

    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.select("ubrn", "ern"),
      Seq("ubrn"), "left_outer")

    getAllLUs(joinedLUs)
  }

  def getAllEntsCalculated(allLinksLusDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                          (implicit spark: SparkSession): Dataset[Row] = {

    val calculatedDF = calculate(allLinksLusDF).castAllToString()
    calculatedDF.cache()

    val existingEntDF = getExistingEntsDF(ConfigOptions.hbaseConfiguration)

    val existingEntCalculatedDF: DataFrame = {
      val calculatedExistingEnt = existingEntDF.join(calculatedDF, Seq("ern"), "left_outer")
      val existingEntsWithRegionRecalculatedDF = calculateRegion(calculatedExistingEnt, regionsByPostcodeDF, regionsByPostcodeShortDF)
      val existingEntsWithEmploymentRecalculatedDF = calculateEmployment(existingEntsWithRegionRecalculatedDF)
      val withReorderedColumns = {
        val columns = completeEntSchema.fieldNames
        existingEntsWithEmploymentRecalculatedDF.select(columns.head, columns.tail: _*)
      }
      spark.createDataFrame(withReorderedColumns.rdd, completeEntSchema)
    }
    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")), Seq("ern"), "left_anti")
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"), "left_outer")

    val newLeusWithWorkingPropsAndRegionDF = calculateDynamicValues(newLEUsCalculatedDF, regionsByPostcodeDF, regionsByPostcodeShortDF)

    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLeusWithWorkingPropsAndRegionDF).rdd, completeEntSchema)
    val newLegalUnitsDF: DataFrame = getNewLeusDF(newLeusWithWorkingPropsAndRegionDF)
    newLegalUnitsDF.cache() //TODO: check if this is actually needed
    newLegalUnitsDF.createOrReplaceTempView(newLeusViewName)

    val allEntsDF = existingEntCalculatedDF.union(newEntsCalculatedDF)

    calculatedDF.unpersist()
    allEntsDF
  }

  def getNewLeusDF(newLEUsCalculatedDF: DataFrame)
                  (implicit spark: SparkSession): DataFrame = {
    val newLegalUnitsDS: RDD[Row] = newLEUsCalculatedDF.rdd.map(row => new GenericRowWithSchema(Array(

      row.getAs[String]("ubrn"),
      row.getAs[String]("ern"),
      generatePrn(row),
      row.getValueOrNull("crn"),
      row.getValueOrEmptyStr("name"),
      row.getValueOrNull("trading_style"), //will not be present
      row.getValueOrEmptyStr("address1"),
      row.getValueOrNull("address2"),
      row.getValueOrNull("address3"),
      row.getValueOrNull("address4"),
      row.getValueOrNull("address5"),
      row.getValueOrEmptyStr("postcode"),
      row.getValueOrEmptyStr("industry_code"),
      row.getValueOrNull("paye_jobs"),
      row.getValueOrNull("turnover"),
      row.getValueOrEmptyStr("legal_status"),
      row.getValueOrNull("trading_status"),
      row.getValueOrEmptyStr("birth_date"),
      row.getValueOrNull("death_date"),
      row.getValueOrNull("death_code"),
      row.getValueOrNull("uprn")
    ), leuRowSchema))

    spark.createDataFrame(newLegalUnitsDS, leuRowSchema)

  }

  def recalculateLouEmploymentAndRegion(ruDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame, confs: Configuration)
                                       (implicit spark: SparkSession): Dataset[Row] = {

    val existingLous: DataFrame = getExistingLousDF(confs)
    val lousWithEmploymentReCalculated = existingLous.drop("employment").join(ruDF.select(col("rurn"), col("employment")), Seq("rurn"), "inner")
    val lousWithRegionRecalcuated = calculateRegion(lousWithEmploymentReCalculated, regionsByPostcodeDF, regionsByPostcodeShortDF)

    lousWithRegionRecalcuated
  }

  def getAllRus(allEntsDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame, confs: Configuration)
               (implicit spark: SparkSession): Dataset[Row] = {

    val existingRUs: DataFrame = getExistingRusDF(confs)

    //val existingRusWithRecalculatedEmployment =
    val columns = ruRowSchema.fieldNames
    val ruWithRegion: DataFrame = calculateRegion(existingRUs, regionsByPostcodeDF, regionsByPostcodeShortDF).select(columns.head, columns.tail: _*)
    val entsWithoutRus: DataFrame = allEntsDF.join(ruWithRegion.select("ern"), Seq("ern"), "left_anti")
    val newAndMissingRusDF: DataFrame = createNewRus(entsWithoutRus).select(columns.head, columns.tail: _*)
    val res = ruWithRegion.union(newAndMissingRusDF)

    res
  }

  def getAllLous(allRus: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame, confs: Configuration)
                (implicit spark: SparkSession): Dataset[Row] = {

    val columns = louRowSchema.fieldNames
    val existingLous: DataFrame = getExistingLousDF( confs)
    val existingLousWithRegion: DataFrame = calculateRegion(existingLous, regionsByPostcodeDF, regionsByPostcodeShortDF).select(columns.head, columns.tail: _*)

    val rusWithoutLous: DataFrame = allRus.join(existingLousWithRegion.select("rurn"), Seq("rurn"), "left_anti")
    val newAndMissingLousDF: DataFrame = createNewLous(rusWithoutLous)

    existingLousWithRegion.union(newAndMissingLousDF)
  }

  def getAllLeus(confs: Configuration)(implicit spark: SparkSession): Dataset[Row] = {
    val existingLEUs: DataFrame = getExistingLeusDF(confs)
    val newLeusDF = spark.sql(s"""SELECT * FROM $newLeusViewName""")
    existingLEUs.union(newLeusDF)
  }
}

object AssembleUnitsClosure$ extends AssembleUnitsClosure