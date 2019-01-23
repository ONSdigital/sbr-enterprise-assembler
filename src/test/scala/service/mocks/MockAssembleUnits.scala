package service.mocks

import dao.DaoUtils._
import dao.hbase.HBaseDataReader.readEntitiesFromHFile
import model.{HFileRow, Schemas}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import service.calculations.Calculate
import service.mocks.MockCreateNewPeriodHBaseDao.adjustPathToExistingRecords
import util.configuration.AssemblerConfiguration
import utils.data.TestIds

object MockAssembleUnits extends Calculate with TestIds {
  override def getExistingLinksLeusDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(AssemblerConfiguration.PathToLinksHFile)
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](path).filter(_.key.startsWith("LEU~"))
    val rdd: RDD[Row] = hfileRows.sortBy(_.cells.map(_.column).mkString).map(_.toLeuLinksRow)
    spark.createDataFrame(rdd, Schemas.linksLeuRowSchema)
  }

  override def getExistingEntsDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(AssemblerConfiguration.PathToEnterpriseHFile)
    val ents: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toEntRow)
    spark.createDataFrame(ents, Schemas.entRowSchema)
  }

  override def generateErn(row: Row): String =
  ernMapping(row.getString("name").get)


  override def generatePrn(row: Row): String = {
    val key = row.getString("name").get
    prnMapping(key)
  }


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
