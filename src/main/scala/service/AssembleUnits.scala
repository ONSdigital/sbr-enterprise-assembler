package service

import java.util.Calendar

import dao.DaoUtils._
import dao.hbase.HBaseDao
import dao.hive.HiveDao
import model.Schemas
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
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

  def createUnitsHfiles(implicit spark: SparkSession, con: Connection): Unit = {

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
    println(s"Partitions size: allLinksLeusDF: ${allLinksLeusDF.rdd.partitions.length}")

    val allEntsDF = getAllEntsCalculated(allLinksLeusDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache()
    println(s"Partitions size: allEntsDF: ${allEntsDF.rdd.partitions.length}")

    val allRusDF = getAllRus(allEntsDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache()
    println(s"Partitions size: allRusDF: ${allRusDF.rdd.partitions.length}")

    val allLousDF = getAllLous(allRusDF, regionsByPostcodeDF, regionsByPostcodeShortDF, hbaseConfiguration).cache()
    println(s"Partitions size: allLousDF: ${allLousDF.rdd.partitions.length}")

    val allLeusDF = getAllLeus(hbaseConfiguration).cache()
    println(s"Partitions size: allLeusDF: ${allLeusDF.rdd.partitions.length}")

    HBaseDao.truncateTables

    val t1 = new Thread {
      override def run(): Unit = {
        saveEnts(allEntsDF)
        HBaseDao.loadEnterprisesHFile
      }
    }

    val t2 = new Thread {
      override def run(): Unit = {
        saveRus(allRusDF)
        HBaseDao.loadRusHFile
      }
    }

    val t3 = new Thread {
      override def run(): Unit = {
        saveLous(allLousDF)
        HBaseDao.loadLousHFile
      }
    }

    val t4 = new Thread {
      override def run(): Unit = {
        saveLeus(allLeusDF)
        HBaseDao.loadLeusHFile
      }
    }

    val t5 = new Thread {
      override def run(): Unit = {
        saveLinks(allLousDF, allRusDF, allLinksLeusDF)
        HBaseDao.loadLinksHFile
      }
    }

    println(s"${Calendar.getInstance.getTime} --> Start threads")
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    println(s"${Calendar.getInstance.getTime} --> Wait for threads")
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    println(s"${Calendar.getInstance.getTime} --> Threads done")
    //saveEnts(allEntsDF)
    //saveRus(allRusDF)
    //saveLous(allLousDF)
    //saveLeus(allLeusDF)
    //saveLinks(allLousDF, allRusDF, allLinksLeusDF)

    println(s"${Calendar.getInstance.getTime} --> unpersist start")
    allLeusDF.unpersist()
    allLousDF.unpersist()
    allRusDF.unpersist()
    allEntsDF.unpersist()
    allLinksLeusDF.unpersist()
    regionsByPostcodeDF.unpersist()

    println(s"${Calendar.getInstance.getTime} --> unpersist done")
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

    println(s"${Calendar.getInstance.getTime} --> Starting: parallelism is: $partitions")
    val calculatedDF = CalculateAdminData(allLinksLusDF).castAllToString
    calculatedDF.cache()
    println(s"${Calendar.getInstance.getTime} --> Partitions size: calculatedDF: ${calculatedDF.rdd.partitions.length}")

    val existingEntDF = getExistingEntsDF(hbaseConfiguration)
    println(s"${Calendar.getInstance.getTime} --> Partitions size: existingEntDF: ${existingEntDF.rdd.partitions.length}")

    val existingEntCalculatedDF: DataFrame = {
      val calculatedExistingEnt = existingEntDF.join(calculatedDF, Seq("ern"), "left_outer").coalesce(partitions)
      println(s"${Calendar.getInstance.getTime} --> Partitions size: calculatedExistingEnt: ${calculatedExistingEnt.rdd.partitions.length}")

      val existingEntsWithRegionRecalculatedDF = CalculateRegion(calculatedExistingEnt, regionsByPostcodeDF, regionsByPostcodeShortDF).coalesce(partitions)
      println(s"${Calendar.getInstance.getTime} --> Partitions size: existingEntsWithRegionRecalculatedDF: ${existingEntsWithRegionRecalculatedDF.rdd.partitions.length}")

      val existingEntsWithEmploymentRecalculatedDF = CalculateEmployment(existingEntsWithRegionRecalculatedDF).coalesce(partitions)
      println(s"${Calendar.getInstance.getTime} --> Partitions size: existingEntsWithEmploymentRecalculatedDF: ${existingEntsWithEmploymentRecalculatedDF.rdd.partitions.length}")

      val withReorderedColumns = {
        val columns = Schemas.completeEntSchema.fieldNames
        existingEntsWithEmploymentRecalculatedDF.select(columns.head, columns.tail: _*)
      }
      println(s"${Calendar.getInstance.getTime} --> Partitions size: withReorderedColumns: ${withReorderedColumns.rdd.partitions.length}")

      spark.createDataFrame(withReorderedColumns.rdd, Schemas.completeEntSchema)
    }.coalesce(partitions)
    println(s"${Calendar.getInstance.getTime} --> Partitions size: existingEntCalculatedDF: ${existingEntCalculatedDF.rdd.partitions.length}")

    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")), Seq("ern"), "left_anti").coalesce(partitions)
    println(s"${Calendar.getInstance.getTime} --> Partitions size: newLEUsDF: ${newLEUsDF.rdd.partitions.length}")

    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"), "left_outer").coalesce(partitions)
    println(s"${Calendar.getInstance.getTime} --> Partitions size: newLEUsCalculatedDF: ${newLEUsCalculatedDF.rdd.partitions.length}")

    val newLeusWithWorkingPropsAndRegionDF = CalculateDynamicValues(newLEUsCalculatedDF, regionsByPostcodeDF, regionsByPostcodeShortDF)
    println(s"${Calendar.getInstance.getTime} --> Partitions size: newLeusWithWorkingPropsAndRegionDF: ${newLeusWithWorkingPropsAndRegionDF.rdd.partitions.length}")

    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLeusWithWorkingPropsAndRegionDF).rdd, Schemas.completeEntSchema).coalesce(partitions)
    println(s"${Calendar.getInstance.getTime} --> Partitions size: newEntsCalculatedDF: ${newEntsCalculatedDF.rdd.partitions.length}")

    val newLegalUnitsDF: DataFrame = getNewLeusDF(newLeusWithWorkingPropsAndRegionDF)
    newLegalUnitsDF.cache() //TODO: check if this is actually needed
    newLegalUnitsDF.createOrReplaceTempView(newLeusViewName)
    println(s"${Calendar.getInstance.getTime} --> Partitions size: newLegalUnitsDF: ${newLegalUnitsDF.rdd.partitions.length}")

    val allEntsDF = existingEntCalculatedDF.union(newEntsCalculatedDF)
    println(s"${Calendar.getInstance.getTime} --> Partitions size: allEntsDF: ${allEntsDF.rdd.partitions.length}")
    calculatedDF.unpersist()
    allEntsDF.repartition(partitions)
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

    val partitions = spark.sparkContext.defaultParallelism * 2
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