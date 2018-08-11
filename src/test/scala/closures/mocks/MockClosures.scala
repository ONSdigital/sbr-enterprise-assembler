package closures.mocks

import closures.BaseClosure
import closures.mocks.MockCreateNewPeriodHBaseDao.adjustPathToExistingRecords
import dao.hbase.HFileUtils
import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SparkSession}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql.{SqlRowExtensions, entRowSchema, louRowSchema, luRowSchema}


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

  override def generateErn(row: Row, appParams: AppParams) = ernMapping(row.getString("BusinessName").get)

  override def generateLurn(row: Row, appParams: AppParams) = {
    val key  = Seq(row.getString("BusinessName"),row.getString("name")).collect{case Some(name) => name}.head
    lurnMapping(key)
  }

  override def generateLurnFromEnt(row: Row, appParams: AppParams) = lurnMapping(generateLurn(row, appParams))


  override def getExistingLeusDF(appParams: AppParams,confs: Configuration )(implicit spark: SparkSession) = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_LINKS_HFILE)
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](path).filter(_.key.startsWith("LEU~"))
    val rdd: RDD[Row] = hfileRows.sortBy(_.cells.map(_.column).mkString).map(_.toLuRow)
    spark.createDataFrame(rdd, luRowSchema)

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

