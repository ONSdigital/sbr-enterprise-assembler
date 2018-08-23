package closures.mocks

import closures.BaseClosure
import closures.mocks.MockCreateNewPeriodHBaseDao.adjustPathToExistingRecords
import dao.hbase.HFileUtils
import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql._


trait MockClosures{this:BaseClosure with HFileUtils =>

  val ernMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "111111111-TEST-ERN"),
    ("ACCLAIMED HOMES LIMITED" ->       "222222222-TEST-ERN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "333333333-TEST-ERN"),
    ("NEW ENTERPRISE LU" -> "444444444-TEST-ERN")
  )

  val lurnMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "11111111-TEST-LURN"),
    ("ACCLAIMED HOMES LIMITED" ->       "22222222-TEST-LURN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "33333333-TEST-LURN"),
    ("NEW ENTERPRISE LU" -> "444444444-TEST-LURN")
  )

  val rurnMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "111111111-TEST-RURN"),
    ("ACCLAIMED HOMES LIMITED" ->       "222222222-TEST-RURN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "333333333-TEST-RURN"),
    ("NEW ENTERPRISE LU" -> "444444444-TEST-RURN")
  )
  val prnMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "111111111-TEST-PRN"),
    ("ACCLAIMED HOMES LIMITED" ->       "222222222-TEST-PRN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "333333333-TEST-PRN"),
    ("NEW ENTERPRISE LU" -> "444444444-TEST-PRN")
  )

  override def generateErn(row: Row, appParams: AppParams) = ernMapping(row.getString("BusinessName").get)

  override def generateLurn(row: Row, appParams: AppParams) = {
    val key  = Seq(row.getString("BusinessName"),row.getString("name")).collect{case Some(name) => name}.head
    lurnMapping(key)
  }
  override def generateRurn(row: Row, appParams: AppParams) = {
    val key  = Seq(row.getString("BusinessName"),row.getString("name")).collect{case Some(name) => name}.head
    rurnMapping(key)
  }

  override def generatePrn(row: Row, appParams: AppParams) = {
    val key  = Seq(row.getString("BusinessName"),row.getString("name")).collect{case Some(name) => name}.head
    prnMapping(key)
  }

  override def generateLurnFromEnt(row: Row, appParams: AppParams) = lurnMapping(generateLurn(row, appParams))

  override def getExistingLinksLeusDF(appParams: AppParams,confs: Configuration )(implicit spark: SparkSession) = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_LINKS_HFILE)
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](path).filter(_.key.startsWith("LEU~"))
    val rdd: RDD[Row] = hfileRows.sortBy(_.cells.map(_.column).mkString).map(_.toLeuLinksRow)
    spark.createDataFrame(rdd, linksLeuRowSchema)
  }

  override def getExistingLeusDF(appParams: AppParams,confs: Configuration )(implicit spark: SparkSession) = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_LEGALUNITS_HFILE)
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](path)
    val rdd: RDD[Row] = hfileRows.sortBy(_.cells.map(_.column).mkString).map(_.toLeuRow)
    spark.createDataFrame(rdd, leuRowSchema)
  }

  override def getExistingRusDF(appParams: AppParams,confs: Configuration )(implicit spark: SparkSession) = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_REPORTINGUNITS_HFILE)
    val rus: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toRuRow)
    spark.createDataFrame(rus, ruRowSchema)
  }

  override def getExistingLousDF(appParams: AppParams,confs: Configuration )(implicit spark: SparkSession) = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_LOCALUNITS_HFILE)
    val lous: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toLouRow)
    spark.createDataFrame(lous, louRowSchema)
  }

  override def getExistingEntsDF(appParams: AppParams,confs: Configuration )(implicit spark: SparkSession) = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_ENTERPRISE_HFILE)
    val ents: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toEntRow)
    spark.createDataFrame(ents, entRowSchema)
  }


}

