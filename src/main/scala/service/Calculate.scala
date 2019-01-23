package service.calculations

import dao.AssembleDao
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import dao.DaoUtils._
import dao.hive.HiveDao
import model.Schemas
import util.configuration.AssemblerConfiguration
import util.configuration.AssemblerHBaseConfiguration.hbaseConfiguration

class Calculate extends AssembleDao with Serializable {

  @transient private lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")

  def createHfiles(implicit spark: SparkSession, con: Connection): Unit = {
    log.debug("Creating HFiles")

    val regionsByPostcodeDF: DataFrame = if (AssemblerConfiguration.isLocal) {
      spark.read.option("header", "true").csv(AssemblerConfiguration.PathToGeo).select("pcds", "rgn").toDF("postcode", "region").cache()
    } else {
      HiveDao.getRegions.cache()
    }

    val regionsByPostcodeShortDF: DataFrame = if (AssemblerConfiguration.isLocal) {
      spark.read.option("header", "true").csv(AssemblerConfiguration.PathToGeoShort).select("pcds", "rgn").toDF("postcodeout", "region").cache()
    } else {
      HiveDao.getRegionsShort.cache()
    }

    val allLinksLeusDF = getAllLinksLUsDF().cache()

    val allAdminDataDF = getAdminDataCalculated(allLinksLeusDF).cache()

    val allRegionDF = getRegionCalculated(allLinksLeusDF, allAdminDataDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache()

    val allEmploymentDF = getEmploymentCalculated(allLinksLeusDF, allAdminDataDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache()

    saveAdminData(allAdminDataDF)
    saveRegion(allRegionDF)
    saveEmployment(allEmploymentDF)

    allAdminDataDF.unpersist(false)
    allRegionDF.unpersist(false)
    allEmploymentDF.unpersist(false)
    allLinksLeusDF.unpersist(false)
    regionsByPostcodeDF.unpersist(false)
    log.debug("HFiles created")
  }



  private def getAllLinksLUsDF()(implicit spark: SparkSession): DataFrame = {

    log.debug("Start getAllLinksLUsDF")

    val incomingBiDataDF: DataFrame = getIncomingBiData()
    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF(hbaseConfiguration)

    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.select("ubrn", "ern"),
      Seq("ubrn"), "left_outer")

    log.debug("End getAllLinksLUsDF")
    getAllLUs(joinedLUs)
  }


  private def getAdminDataCalculated(allLinksLusDF: DataFrame)
                                    (implicit spark: SparkSession): Dataset[Row] = {

    log.debug("Start getAdminDataCalculated")

    val calculatedDF = CalculateAdminData(allLinksLusDF).castAllToString
    val withReorderedColumns = {
      val columns = Schemas.adminDataSchema.fieldNames
      calculatedDF.select(columns.head, columns.tail: _*)
    }
    withReorderedColumns
  }

  private def getRegionCalculated(allLinksLusDF: DataFrame, calculatedDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                                  (implicit spark: SparkSession): Dataset[Row] = {

    val existingEntDF = getExistingEntsDF(hbaseConfiguration)

    log.debug("--> Start existingEntCalculatedDF")
    val existingEntRegionCalculatedDF: DataFrame = {
      val calculatedExistingEnt = existingEntDF.join(calculatedDF, Seq("ern"), "left_outer")
      val existingEntsWithRegionRecalculatedDF = CalculateRegion(calculatedExistingEnt, regionsByPostcodeDF, regionsByPostcodeShortDF)

      val withReorderedColumns = {
        val columns = Schemas.regionSchema.fieldNames
        existingEntsWithRegionRecalculatedDF.select(columns.head, columns.tail: _*)
      }

      spark.createDataFrame(withReorderedColumns.rdd, Schemas.regionSchema)
    }

    val newLEUsDF = allLinksLusDF.join(existingEntRegionCalculatedDF.select(col("ern")), Seq("ern"), "left_anti")
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"), "left_outer")
    val newLeusWithRegionDF = CalculateDynamicValues(newLEUsCalculatedDF, regionsByPostcodeDF, regionsByPostcodeShortDF)

    val regionDF = (existingEntRegionCalculatedDF.select("ern", "region")).union(newLeusWithRegionDF.select("ern", "region"))

    regionDF
  }

  private def getEmploymentCalculated(allLinksLusDF: DataFrame, calculatedDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                                 (implicit spark: SparkSession): Dataset[Row] = {

    val existingEntDF = getExistingEntsDF(hbaseConfiguration)

    log.debug("--> Start existingEntCalculatedDF")
    val existingEntEmploymentCalculatedDF: DataFrame = {
      val calculatedExistingEnt = existingEntDF.join(calculatedDF, Seq("ern"), "left_outer")
      val existingEntsWithRegionRecalculatedDF = CalculateRegion(calculatedExistingEnt, regionsByPostcodeDF, regionsByPostcodeShortDF)
      val existingEntsWithEmploymentRecalculatedDF = CalculateEmployment(existingEntsWithRegionRecalculatedDF)

      val withReorderedColumns = {
        val columns = Schemas.completeEntSchema.fieldNames
        existingEntsWithEmploymentRecalculatedDF.select(columns.head, columns.tail: _*)
      }

      spark.createDataFrame(withReorderedColumns.rdd, Schemas.completeEntSchema)
    }

    val newLEUsDF = allLinksLusDF.join(existingEntEmploymentCalculatedDF.select(col("ern")), Seq("ern"), "left_anti")
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"), "left_outer")
    val newLeusWithRegionDF = CalculateDynamicValues(newLEUsCalculatedDF, regionsByPostcodeDF, regionsByPostcodeShortDF)
    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLeusWithRegionDF).rdd, Schemas.completeEntSchema)

    val employmentDF = (existingEntEmploymentCalculatedDF.select("ern", "employment")).union(newEntsCalculatedDF.select("ern", "employment"))

    employmentDF
  }
}

object Calculate extends Calculate
