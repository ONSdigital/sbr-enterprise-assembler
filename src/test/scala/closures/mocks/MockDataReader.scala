package closures.mocks

import closures.BaseClosure
import closures.mocks.MockCreateNewPeriodHBaseDao.adjustPathToExistingRecords
import global.{AppParameters, AppParams}
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql._

trait MockDataReader extends BaseClosure {

  override def getExistingLinksLeusDF( confs: Configuration )(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(AppParams.PATH_TO_LINKS_HFILE)
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](path).filter(_.key.startsWith("LEU~"))
    val rdd: RDD[Row] = hfileRows.sortBy(_.cells.map(_.column).mkString).map(_.toLeuLinksRow)
    spark.createDataFrame(rdd, linksLeuRowSchema)
  }

  override def getExistingLeusDF(confs: Configuration )(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(AppParams.PATH_TO_LEGALUNITS_HFILE)
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](path)
    val rdd: RDD[Row] = hfileRows.sortBy(_.cells.map(_.column).mkString).map(_.toLeuRow)
    spark.createDataFrame(rdd, leuRowSchema)
  }

  override def getExistingRusDF(confs: Configuration )(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(AppParams.PATH_TO_REPORTINGUNITS_HFILE)
    val rus: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toRuRow)
    spark.createDataFrame(rus, ruRowSchema)
  }

  override def getExistingLousDF(aconfs: Configuration )(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(AppParams.PATH_TO_LOCALUNITS_HFILE)
    val lous: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toLouRow)
    spark.createDataFrame(lous, louRowSchema)
  }

  override def getExistingEntsDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val path = adjustPathToExistingRecords(AppParams.PATH_TO_ENTERPRISE_HFILE)
    val ents: RDD[Row] = readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).map(_.toEntRow)
    spark.createDataFrame(ents, entRowSchema)
  }

}
