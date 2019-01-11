package service

import java.util.Calendar

import dao.DaoUtils._
import dao.hbase.HBaseDao
import dao.hive.HiveDao
import dao.parquet.ParquetDao.getClass
import model.Schemas
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import service.calculations.{CalculateAdminData, CalculateDynamicValues, CalculateEmployment, CalculateRegion}
import util.configuration.AssemblerConfiguration
import util.configuration.AssemblerHBaseConfiguration._

trait AssembleUnits extends BaseUnits with Serializable {

  val newRusViewName = "NEWRUS"
  val newLeusViewName = "NEWLEUS"

  def createHfiles(implicit spark: SparkSession, con: Connection): Unit = {

    val regionsByPostcodeDF: DataFrame = if (AssemblerConfiguration.isLocal) {
      spark.read.option("header", "true").csv(AssemblerConfiguration.PathToGeo).select("pcds", "rgn").toDF("postcode", "region").cache()
    } else {
      HiveDao.getRegions.cache()
    }

    val regionsByPostcodeShortDF: DataFrame = if (AssemblerConfiguration.isLocal) {
      spark.read.option("header", "true").csv(AssemblerConfiguration.PathToGeoShort).select("pcds", "rgn").toDF("postcodeout", "region").cache()
    } else {
      HiveDao.getRegionsShort
    }

    regionsByPostcodeDF.collect()

    val allLinksLeusDF = getAllLinksLUsDF().cache()

    val allEntsDF = getAllEntsCalculated(allLinksLeusDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache()

    val allRusDF = getAllRus(allEntsDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache()

    val allLousDF = getAllLous(allRusDF, regionsByPostcodeDF, regionsByPostcodeShortDF, hbaseConfiguration).cache()

    val allLeusDF = getAllLeus(hbaseConfiguration).cache()

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

  def loadHFiles()(implicit spark: SparkSession, con: Connection) : Unit = {
    println(s"${Calendar.getInstance.getTime} --> Start load")
    HBaseDao.truncateTables
    HBaseDao.loadLinksHFile
    HBaseDao.loadEnterprisesHFile
    HBaseDao.loadLousHFile
    HBaseDao.loadLeusHFile
    HBaseDao.loadRusHFile
    println(s"${Calendar.getInstance.getTime} --> End load")
  }

  private def getAllLinksLUsDF()(implicit spark: SparkSession): DataFrame = {

    val partitions = spark.sparkContext.defaultParallelism * 2
    val incomingBiDataDF: DataFrame = getIncomingBiData().coalesce(partitions)

    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF(hbaseConfiguration).coalesce(partitions)

    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.select("ubrn", "ern"),
      Seq("ubrn"), "left_outer").coalesce(partitions)

    getAllLUs(joinedLUs).coalesce(partitions)
  }

  private def getAllEntsCalculated(allLinksLusDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                                  (implicit spark: SparkSession): Dataset[Row] = {

    val partitions = spark.sparkContext.defaultParallelism

    val calculatedDF = CalculateAdminData(allLinksLusDF).castAllToString
    calculatedDF.cache()

    val existingEntDF = getExistingEntsDF(hbaseConfiguration)

    val existingEntCalculatedDF: DataFrame = {
      val calculatedExistingEnt = existingEntDF.join(calculatedDF, Seq("ern"), "left_outer").coalesce(partitions)
      val existingEntsWithRegionRecalculatedDF = CalculateRegion(calculatedExistingEnt, regionsByPostcodeDF, regionsByPostcodeShortDF).coalesce(partitions)
      val existingEntsWithEmploymentRecalculatedDF = CalculateEmployment(existingEntsWithRegionRecalculatedDF).coalesce(partitions)

      val withReorderedColumns = {
        val columns = Schemas.completeEntSchema.fieldNames
        existingEntsWithEmploymentRecalculatedDF.select(columns.head, columns.tail: _*)
      }

      spark.createDataFrame(withReorderedColumns.rdd, Schemas.completeEntSchema)
    }.coalesce(partitions)

    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")), Seq("ern"), "left_anti").coalesce(partitions)
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"), "left_outer").coalesce(partitions)
    val newLeusWithWorkingPropsAndRegionDF = CalculateDynamicValues(newLEUsCalculatedDF, regionsByPostcodeDF, regionsByPostcodeShortDF)
    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLeusWithWorkingPropsAndRegionDF).rdd, Schemas.completeEntSchema).coalesce(partitions)

    val newLegalUnitsDF: DataFrame = getNewLeusDF(newLeusWithWorkingPropsAndRegionDF)
    newLegalUnitsDF.cache() //TODO: check if this is actually needed
    newLegalUnitsDF.createOrReplaceTempView(newLeusViewName)

    val allEntsDF = existingEntCalculatedDF.union(newEntsCalculatedDF)
    calculatedDF.unpersist()
    allEntsDF.coalesce(partitions)
  }

  private def getNewLeusDF(newLEUsCalculatedDF: DataFrame)
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
    ), Schemas.leuRowSchema))

    val partitions = spark.sparkContext.defaultParallelism
    spark.createDataFrame(newLegalUnitsDS, Schemas.leuRowSchema).coalesce(partitions)

  }

  private def getAllRus(allEntsDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                       (implicit spark: SparkSession, confs: Configuration): Dataset[Row] = {

    val existingRUs: DataFrame = getExistingRusDF(confs)
    val partitions = spark.sparkContext.defaultParallelism

    val columns = Schemas.ruRowSchema.fieldNames
    val ruWithRegion: DataFrame = CalculateRegion(existingRUs, regionsByPostcodeDF, regionsByPostcodeShortDF).select(columns.head, columns.tail: _*)
    val entsWithoutRus: DataFrame = allEntsDF.join(ruWithRegion.select("ern"), Seq("ern"), "left_anti")
    val newAndMissingRusDF: DataFrame = createNewRus(entsWithoutRus).select(columns.head, columns.tail: _*)
    val res = ruWithRegion.union(newAndMissingRusDF).coalesce(partitions)

    res
  }

  private def getAllLous(allRus: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame, confs: Configuration)
                        (implicit spark: SparkSession): Dataset[Row] = {

    val columns = Schemas.louRowSchema.fieldNames
    val existingLous: DataFrame = getExistingLousDF(confs)
    val existingLousWithRegion: DataFrame = CalculateRegion(existingLous, regionsByPostcodeDF, regionsByPostcodeShortDF).select(columns.head, columns.tail: _*)

    val rusWithoutLous: DataFrame = allRus.join(existingLousWithRegion.select("rurn"), Seq("rurn"), "left_anti")
    val newAndMissingLousDF: DataFrame = createNewLous(rusWithoutLous)

    val partitions = spark.sparkContext.defaultParallelism
    existingLousWithRegion.union(newAndMissingLousDF).coalesce(partitions)
  }

  private def getAllLeus(confs: Configuration)(implicit spark: SparkSession): Dataset[Row] = {
    val partitions = spark.sparkContext.defaultParallelism
    val existingLEUs: DataFrame = getExistingLeusDF(confs)
    val newLeusDF = spark.sql(s"""SELECT * FROM $newLeusViewName""")
    existingLEUs.union(newLeusDF).coalesce(partitions)
  }

}

object AssembleUnits extends AssembleUnits