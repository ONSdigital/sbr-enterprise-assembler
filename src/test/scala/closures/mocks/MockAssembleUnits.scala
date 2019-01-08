package closures.mocks

import closures.AssembleUnits
import closures.mocks.MockCreateNewPeriodHBaseDao.adjustPathToExistingRecords
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql._
import util.options.ConfigOptions
import utils.data.TestIds

object MockAssembleUnits extends AssembleUnits with TestIds {

  override def getExistingLinksLeusDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(ConfigOptions.PathToLinksHfile)
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](path).filter(_.key.startsWith("LEU~"))
    val rdd: RDD[Row] = hfileRows.sortBy(_.cells.map(_.column).mkString).map(_.toLeuLinksRow)
    spark.createDataFrame(rdd, linksLeuRowSchema)
  }

  override def getExistingLeusDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(ConfigOptions.PathToLegalUnitsHFile)
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](path)
    val rdd: RDD[Row] = hfileRows.sortBy(_.cells.map(_.column).mkString).map(_.toLeuRow)
    spark.createDataFrame(rdd, leuRowSchema)
  }

  override def getExistingRusDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(ConfigOptions.PathToReportingUnitsHFile)
    val rus: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toRuRow)
    spark.createDataFrame(rus, ruRowSchema)
  }

  override def getExistingLousDF(aconfs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(ConfigOptions.PathToLocalUnitsHFile)
    val lous: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toLouRow)
    spark.createDataFrame(lous, louRowSchema)
  }

  override def getExistingEntsDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(ConfigOptions.PathToEnterpriseHFile)
    val ents: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toEntRow)
    spark.createDataFrame(ents, entRowSchema)
  }

  override def generateErn(row: Row): String =
    ernMapping(row.getString("name").get)

  override def generateLurn(row: Row): String = {
    val key = row.getString("name").get
    lurnMapping(key)
  }

  override def generateRurn(row: Row): String = {
    val key = row.getString("name").get
    rurnMapping(key)
  }

  override def generatePrn(row: Row): String = {
    val key = row.getString("name").get
    prnMapping(key)
  }

  override def generateLurnFromEnt(row: Row): String =
    lurnMapping(generateLurn(row))

  val ernMapping: Map[String, String] = Map(
    "NEW ENTERPRISE LU" -> newEntErn
  )

  val lurnMapping: Map[String, String] = Map(
    "NEW ENTERPRISE LU" -> newLouLurn
  )

  val rurnMapping: Map[String, String] = Map(
    "NEW ENTERPRISE LU" -> newRuRurn
  )

  val prnMapping: Map[String, String] = Map(
    "NEW ENTERPRISE LU" -> newRuPrn
  )
}