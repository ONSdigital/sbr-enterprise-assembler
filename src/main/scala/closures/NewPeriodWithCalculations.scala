package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import global.{AppParams, Configs}
import model.domain.{HFileRow, KVCell}
import model.hfile
import model.hfile.{HFileCell, Tables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.RddLogging
import spark.calculations.{AdminDataCalculator, DataFrameHelper}
import spark.extensions.sql._

import scala.util.Try

trait NewPeriodWithCalculations extends WithConversionHelper with DataFrameHelper with RddLogging with Serializable{
  val hbaseDao: HBaseDao = HBaseDao

  type Cells = Iterable[KVCell[String, String]]
  type Record = (String, Cells)


  def getPreCalculatedLuDF(appconf: AppParams)(implicit spark: SparkSession):Unit = {

    val confs: Configuration = Configs.conf

    val updatedConfs = appconf.copy(TIME_PERIOD = appconf.PREVIOUS_TIME_PERIOD)

    val joinedLUs: RDD[(String, (Option[Cells], Option[Cells]))] = getJoinedLUs(appconf,confs).cache()

    val allLUsRows: RDD[Row] = getAllLUs(joinedLUs)
    val allLUsDF = spark.createDataFrame(allLUsRows, preCalculateDfSchema)

    val preCalculatedLuDF = AdminDataCalculator.aggregateDF(allLUsDF)

    preCalculatedLuDF
  }

  def getAllLUs(joinedLUs: DataFrame)(implicit spark: SparkSession) = {
    val dataset: Dataset[Row] = joinedLUs.map{
      case row if(Try{row.getAs[String]("ern")}.isFailure) => {
        val ern = generateErn(null,null)
        Row(Array(
          ern,
          row.getAs[String]("id"),
          row.getAs[String]("BusinessName"),
          row.getAs[String]("IndustryCode"),
          row.getAs[String]("LegalStatus"),
          row.getAs[String]("PostCode"),
          row.getAs[String]("TradingStatus"),
          row.getAs[String]("Turnover"),
          row.getAs[String]("UPRN"),
          row.getAs[String]("CompanyNo"),
          row.getAs[Seq[String]]("PayeRefs"),
          row.getAs[Seq[String]]("VatRefs")
        ))

      }}
    spark.createDataFrame(dataset.rdd,biWithErnSchema)
  } /*{

      case (key, (Some(newCells), Some(oldCells))) => toRow(key,newCells,oldCells)
      case (key, (Some(newCells), None)) if(key.contains("~LEU~")) => toRow(key,newCells)
    }
*/
  def toRow(key:String, newCells:Cells, oldCells:Cells):Row = {

    val cells = newCells ++ oldCells.find(_.column=="p_ENT").map(ernCell => Seq(ernCell)).getOrElse(Seq.empty)
    val ern = cells.collectFirst{case KVCell("p_ENT",value) => value}.get //it must be present
    val id = key.split("~").head
    val payeRefs = cells.collect{case KVCell(key,"PAYE") => key.replace(childPrefix, "")}
    val vatRefs = cells.collect{case KVCell(key,"VAT") => key.replace(childPrefix, "")}
    new GenericRowWithSchema(Array(
                                    ern,
                                    id,
                                    payeRefs,
                                    vatRefs
                                  ), preCalculateDfSchema)
  }

  def toRow(key:String, cells:Cells):Row = {
    val ern = generateErn(null,null)
    val id = key.split("~").head
    val payeRefs = cells.collect{case KVCell(key,"PAYE") => key.replace(childPrefix, "")}
    val vatRefs = cells.collect{case KVCell(key,"VAT") => key.replace(childPrefix, "")}
    new GenericRowWithSchema(Array(
                                  ern,
                                  id,
                                  payeRefs,
                                  vatRefs
                                ), preCalculateDfSchema)
  }

 /* def getNewLUsPreCalculatedDF(joinedLUs: RDD[(String, (Option[Cells], Option[Cells]))])(implicit spark: SparkSession) = {

    val newLUs: RDD[HFileRow] = getNewLUs(joinedLUs)

    val preCalculatedRows:RDD[Row] = newLUs.map(row => {
      val ern = row.cells.find(_.column=="p_ENT").get //it must be present
      val id = row.key.split("~").head
      val payeRefs = row.cells.collect{case KVCell(key,"PAYE") => key.replace(childPrefix, "")}
      val vatRefs = row.cells.collect{case KVCell(key,"VAT") => key.replace(childPrefix, "")}

      new GenericRowWithSchema(Array(
        ern,
        id,
        payeRefs,
        vatRefs
      ), preCalculateDfSchema)
    }
    )
    val preCalculatedDF = spark.createDataFrame(preCalculatedRows,preCalculateDfSchema)
    preCalculatedDF
  }

  def getExistingLUsPreCalculatedDF(joinedLUs: RDD[(String, (Option[Cells], Option[Cells]))])(implicit spark: SparkSession) = {

    val updatedExistingLUs: RDD[HFileRow] = getExistingLUs(joinedLUs)

    val preCalculatedRows:RDD[Row] = updatedExistingLUs.map(row => {
      val ern = row.cells.find(_.column=="p_ENT").get //it must be present
      val id = row.key.split("~").head
      val payeRefs = row.cells.collect{case KVCell(key,"PAYE") => key.replace(childPrefix, "")}
      val vatRefs = row.cells.collect{case KVCell(key,"VAT") => key.replace(childPrefix, "")}

      new GenericRowWithSchema(Array(
        ern,
        id,
        payeRefs,
        vatRefs
      ), preCalculateDfSchema)
    }
    )
    val preCalculatedDF = spark.createDataFrame(preCalculatedRows,preCalculateDfSchema)
    preCalculatedDF
  }

  def getNewLUs(joined:RDD[(String, (Option[Cells], Option[Cells]))])(implicit spark: SparkSession) = {

    val newLUs: RDD[HFileRow] = joined.collect { case (key, (Some(newCells), None)) => HFileRow(key, newCells) }
    newLUs
  }

  def getExistingLUs(joined:RDD[(String, (Option[Cells], Option[Cells]))])(implicit spark: SparkSession) = {

    val updatedExistingLUs: RDD[HFileRow] = joined.collect {
      case (key, (Some(newCells), Some(oldCells))) => HFileRow(key,{newCells ++ oldCells.find(_.column=="p_ENT").map(ernCell => Seq(ernCell)).getOrElse(Seq.empty) })
    }

    updatedExistingLUs
  }*/


  def getJoinedLUs(appconf: AppParams, confs: Configuration)(implicit spark: SparkSession) = {

    val parquetDF = spark.read.parquet(appconf.PATH_TO_PARQUET)

    val castedToStringParquetDF = parquetDF.castAllToString

    val linksRecords: RDD[(String, HFileCell)] = castedToStringParquetDF.rdd.flatMap(row => toLinksRefreshRecords(row, appconf))
    val updatesRdd: RDD[Record] = linksRecords.groupByKey().map(v => (v._1, v._2.map(kv => KVCell[String, String](kv.qualifier, kv.value))))
    val linksTableName = s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
    val luRegex = ".*(ENT|LEU)~" + {
      appconf.PREVIOUS_TIME_PERIOD
    } + "$"
    val existingLuRddWithNewPeriod: RDD[Record] = hbaseDao.readTableWithKeyFilter(confs, appconf, linksTableName, luRegex).map(row => (row.key.replace(s"~${appconf.PREVIOUS_TIME_PERIOD}", s"~${appconf.TIME_PERIOD}"), row.cells))


    val numOfPartitions = updatesRdd.getNumPartitions

    val joined: RDD[(String, (Option[Cells], Option[Cells]))] = updatesRdd.fullOuterJoin(existingLuRddWithNewPeriod, numOfPartitions)
    joined
  }

}
object NewPeriodWithCalculations extends NewPeriodWithCalculations