package closures.mocks

import closures.BaseClosure
import closures.mocks.MockCreateNewPeriodHBaseDao.adjustPathToExistingRecords
import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql._

trait MockDataReader {this:BaseClosure =>

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
