package service

import dao.DaoUtils._
import dao.hbase.HBaseDao
import dao.hive.HiveDao
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

  @transient lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")

  def createHfiles(implicit spark: SparkSession, con: Connection): Unit = {

    log.info("Creating HFiles")

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

    allLeusDF.unpersist(false)
    allLousDF.unpersist(false)
    allRusDF.unpersist(false)
    allEntsDF.unpersist(false)
    allLinksLeusDF.unpersist(false)
    regionsByPostcodeDF.unpersist(false)
    log.info("HFiles created")
  }

  def loadHFiles()(implicit spark: SparkSession, con: Connection) : Unit = {
    log.info("Start load to HBase")
    HBaseDao.truncateTables
    HBaseDao.loadLinksHFile
    HBaseDao.loadEnterprisesHFile
    HBaseDao.loadLousHFile
    HBaseDao.loadLeusHFile
    HBaseDao.loadRusHFile
    log.info("End load to HBase")
  }

  private def getAllLinksLUsDF()(implicit spark: SparkSession): DataFrame = {

    val incomingBiDataDF: DataFrame = getIncomingBiData()

    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF(hbaseConfiguration)

    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.select("ubrn", "ern"),
      Seq("ubrn"), "left_outer")

    getAllLUs(joinedLUs)
  }

  private def getAllEntsCalculated(allLinksLusDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                                  (implicit spark: SparkSession): Dataset[Row] = {

    val calculatedDF = CalculateAdminData(allLinksLusDF).castAllToString
    calculatedDF.cache()

    val existingEntDF = getExistingEntsDF(hbaseConfiguration)

    val existingEntCalculatedDF: DataFrame = {
      val calculatedExistingEnt = existingEntDF.join(calculatedDF, Seq("ern"), "left_outer")
      val existingEntsWithRegionRecalculatedDF = CalculateRegion(calculatedExistingEnt, regionsByPostcodeDF, regionsByPostcodeShortDF)
      val existingEntsWithEmploymentRecalculatedDF = CalculateEmployment(existingEntsWithRegionRecalculatedDF)

      val withReorderedColumns = {
        val columns = Schemas.completeEntSchema.fieldNames
        existingEntsWithEmploymentRecalculatedDF.select(columns.head, columns.tail: _*)
      }

      spark.createDataFrame(withReorderedColumns.rdd, Schemas.completeEntSchema)
    }

    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")), Seq("ern"), "left_anti")
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"), "left_outer")
    val newLeusWithWorkingPropsAndRegionDF = CalculateDynamicValues(newLEUsCalculatedDF, regionsByPostcodeDF, regionsByPostcodeShortDF)
    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLeusWithWorkingPropsAndRegionDF).rdd, Schemas.completeEntSchema)

    val newLegalUnitsDF: DataFrame = getNewLeusDF(newLeusWithWorkingPropsAndRegionDF)
    newLegalUnitsDF.cache() //TODO: check if this is actually needed
    newLegalUnitsDF.createOrReplaceTempView(newLeusViewName)

    val allEntsDF = existingEntCalculatedDF.union(newEntsCalculatedDF)
    calculatedDF.unpersist(false)
    allEntsDF
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

    spark.createDataFrame(newLegalUnitsDS, Schemas.leuRowSchema)

  }

  private def getAllRus(allEntsDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                       (implicit spark: SparkSession, confs: Configuration): Dataset[Row] = {

    val existingRUs: DataFrame = getExistingRusDF(confs)

    val columns = Schemas.ruRowSchema.fieldNames
    val ruWithRegion: DataFrame = CalculateRegion(existingRUs, regionsByPostcodeDF, regionsByPostcodeShortDF).select(columns.head, columns.tail: _*)
    val entsWithoutRus: DataFrame = allEntsDF.join(ruWithRegion.select("ern"), Seq("ern"), "left_anti")
    val newAndMissingRusDF: DataFrame = createNewRus(entsWithoutRus).select(columns.head, columns.tail: _*)
    val res = ruWithRegion.union(newAndMissingRusDF)

    res
  }

  private def getAllLous(allRus: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame, confs: Configuration)
                        (implicit spark: SparkSession): Dataset[Row] = {

    val columns = Schemas.louRowSchema.fieldNames
    val existingLous: DataFrame = getExistingLousDF(confs)
    val existingLousWithRegion: DataFrame = CalculateRegion(existingLous, regionsByPostcodeDF, regionsByPostcodeShortDF).select(columns.head, columns.tail: _*)

    val rusWithoutLous: DataFrame = allRus.join(existingLousWithRegion.select("rurn"), Seq("rurn"), "left_anti")
    val newAndMissingLousDF: DataFrame = createNewLous(rusWithoutLous)

    existingLousWithRegion.union(newAndMissingLousDF)
  }

  private def getAllLeus(confs: Configuration)(implicit spark: SparkSession): Dataset[Row] = {
    val existingLEUs: DataFrame = getExistingLeusDF(confs)
    val newLeusDF = spark.sql(s"""SELECT * FROM $newLeusViewName""")
    existingLEUs.union(newLeusDF)
  }

}

object AssembleUnits extends AssembleUnits
