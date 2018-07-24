package closures.mocks

import closures.NewPeriodClosure
import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql.existingLuBiRowSchema
import utils.Paths

/**
  *
  */
trait MockNewPeriodClosure extends NewPeriodClosure with Paths{this: { val testDir: String } =>


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
}
