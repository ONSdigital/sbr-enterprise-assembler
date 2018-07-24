package closures.mocks

import closures.NewPeriodClosure
import global.AppParams
import model.domain.{HFileRow, KVCell}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql._
import utils.data.expected.ExpectedDataForAddNewPeriodScenario
import utils.{Paths, TestDataUtils}

/**
  *
  */
trait MockNewPeriodClosure extends NewPeriodClosure with MockClosures  with ExpectedDataForAddNewPeriodScenario with Paths{this: { val testDir: String } =>

    override val hbaseDao = MockCreateNewPeriodHBaseDao


    override def getExistingLEs(appconf:AppParams,confs:Configuration)(implicit spark: SparkSession) = {
      val hfileRows: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).collect.toList.sortBy(_.key)
      val existingLeus: Seq[Row] = hfileRows.filter(_.key.contains("~LEU~")).map(row => Row(
        row.key.split("~").head,
        row.cells.find(_.column=="p_ENT").get.value
      ))
      val existingLinks: RDD[Row] = spark.sparkContext.parallelize(existingLeus)
      val existingLinksDF:DataFrame = spark.createDataFrame(existingLinks,existingLuBiRowSchema)
      existingLinksDF
    }

  //ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
    override def getExistingEntsDF(appconf:AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](existingEntRecordHFiles).sortBy(_.key)
    val existingEnts  = hfileRows.map(row =>
         Row(
              row.cells.find(_.column=="ern").map(_.value).get,
              row.cells.find(_.column=="entref").map(_.value).getOrElse(null),
              row.cells.find(_.column=="name").map(_.value).get,
              row.cells.find(_.column=="trading_style").map(_.value).getOrElse(null),
              row.cells.find(_.column=="address1").map(_.value).getOrElse(""),
              row.cells.find(_.column=="address2").map(_.value).getOrElse(null),
              row.cells.find(_.column=="address3").map(_.value).getOrElse(null),
              row.cells.find(_.column=="address4").map(_.value).getOrElse(null),
              row.cells.find(_.column=="address5").map(_.value).getOrElse(null),
              row.cells.find(_.column=="postcode").map(_.value).getOrElse(""),
              row.cells.find(_.column=="sic07").map(_.value).getOrElse(""),
              row.cells.find(_.column=="legal_status").map(_.value).getOrElse(null)
             ))
    val existingEntsDF:DataFrame = spark.createDataFrame(existingEnts,entRowSchema)
     existingEntsDF
  }
//existingLousRecordHFiles
  override def getExistingLOUsDF(appconf: AppParams, confs: Configuration)(implicit spark: SparkSession) = {
    val hfileRows: RDD[HFileRow] = readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).sortBy(_.key)
    val existingLouRdd = hfileRows.map(row =>
                 Row(
                      row.cells.find(_.column=="lurn").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="luref").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="ern").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="entref").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="name").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="tradingstyle").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="address1").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="address2").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="address3").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="address4").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="address5").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="postcode").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="sic07").map(_.value).getOrElse(null),
                      row.cells.find(_.column=="employees").map(_.value).getOrElse(null)

    ))
    spark.createDataFrame(existingLouRdd, louRowSchema)
  }


}
