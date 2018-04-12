package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import dao.parquet.ParquetDAO
import dao.parquet.ParquetDAO.{toEnterpriseRecords, toLinksRefreshRecords}
import global.{AppParams, Configs}
import model.domain.{HFileRow, KVCell}
import model.hfile
import model.hfile.HFileCell
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import spark.extensions.sql.SqlRowExtensions
import spark.extensions.sql._
import org.apache.spark.sql.SQLContext
import spark.calculations.DataFrameHelper

import scala.util.Try




object CreateNewPeriodClosure extends WithConversionHelper with DataFrameHelper{


  type Cells = Iterable[KVCell[String, String]]
  type Record = (String, Cells)
  /**
    * copies data from Enterprise and Local Units tables into a new period data
    **/
  def createNewPeriodHfiles(confs: Configuration, appParams: AppParams)(implicit spark: SparkSession): Unit = {
    //do enterprise. Local units to follow
    saveEnterpriseHFiles(confs, appParams, ".*ENT~"+{appParams.PREVIOUS_TIME_PERIOD}+"$") //.*(~201802)$ //.*(?!~ENT~)201802$
    saveLinksHFiles(confs, appParams, ".*(~ENT~"+{appParams.PREVIOUS_TIME_PERIOD}+")$")
  }


  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession) = {

    val confs = Configs.conf

    val updatedConfs = appconf.copy(TIME_PERIOD=appconf.PREVIOUS_TIME_PERIOD) //set period to previous to make join possible
    val parquetRows: RDD[Row] = spark.read.parquet(appconf.PATH_TO_PARQUET).rdd
    val linksRecords: RDD[(String, HFileCell)] = parquetRows.flatMap(row => toLinksRefreshRecords(row,appconf))
    val updatesRdd: RDD[Record] = linksRecords.groupByKey().map(v => (v._1,v._2.map(kv => KVCell[String,String](kv.qualifier,kv.value))))//get LINKS updates from input parquet

    //next 3 lines: select LU rows from hbase
    val linksTableName = s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
    val luRegex = ".*(ENT|LEU)~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val existingLuRdd: RDD[Record] = HBaseDao.readTableWithKeyFilter(confs,appconf, linksTableName, luRegex).map(row => (row.key.replace(s"~${appconf.PREVIOUS_TIME_PERIOD}",s"~${appconf.TIME_PERIOD}"),row.cells))

    val joined: RDD[(String, (Option[Cells], Option[Cells]))] = updatesRdd.fullOuterJoin(existingLuRdd, updatesRdd.getNumPartitions)

    val updatedExistingLUs: RDD[HFileRow] = joined.collect { case (key, (Some(newCells), Some(oldCells))) => HFileRow(key,{newCells ++ oldCells.find(_.column=="p_ENT").map(ernCell => Seq(ernCell)).getOrElse(Seq.empty) })} // existing LUs updated with new cells

    val newLUs: RDD[HFileRow] = joined.collect { case (key, (Some(newCells), None)) => HFileRow(key, newCells) }

    val existingLinksEnts: RDD[HFileRow] = joined.collect { case (key, (None, Some(oldCells))) if(key.endsWith(s"ENT~${appconf.TIME_PERIOD}"))=> HFileRow(key, oldCells) }



     //hfile ready links records for existing LUs
   //filter from parquetRows rows with id of new LUs
    val newLuIds: RDD[(Long, Row)] = newLUs.filter(_.key.endsWith(s"~LEU~${appconf.TIME_PERIOD}")).collect{case HFileRow(key,_) if(key.endsWith(s"~${appconf.TIME_PERIOD}")) => (key.stripSuffix(s"~LEU~${appconf.TIME_PERIOD}").toLong,Row.empty)}

    val rowMapByKey: RDD[(Long, Row)] = parquetRows.map(row => (row.getLong("id").get, row))

    val joinedParquetRows = newLuIds.leftOuterJoin(rowMapByKey,newLuIds.getNumPartitions)
    val newLUParquetRows: RDD[Row] = joinedParquetRows.collect{  case (key,(oldRow,Some(newRow))) => newRow }

    val newRowsDf: DataFrame = spark.createDataFrame(newLUParquetRows,parquetRowSchema)

    val newEntTree: RDD[hfile.Tables] = finalCalculations(newRowsDf, spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.map(row => toEnterpriseRecords(row,appconf))

    val newEnts: RDD[(String, HFileCell)] =  newEntTree.flatMap(_.enterprises) //break into cells
    val newLinks: RDD[(String, HFileCell)] =  newEntTree.flatMap(_.links) //break into cells

    //Add new records to existing records
    val linksForExistingLUs: RDD[HFileRow] = updatedExistingLUs.union(existingLinksEnts)  //rows of blah~ENT~[period] AND blah~LEU~[period] records



    val hCellsOfExistingLUs: RDD[(String, HFileCell)] = linksForExistingLUs.flatMap(_.toHfileCells(appconf.HBASE_LINKS_COLUMN_FAMILY))
    //val allLinks = hCellsOfExistingLUs.union(newLinks) // ADD update links NOT new links


    //select ENT rows from hbase
    val entRegex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val entTableName = s"${appconf.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appconf.HBASE_ENTERPRISE_TABLE_NAME}"
    val existingEntRdd: RDD[Row] = HBaseDao.readTableWithKeyFilter(confs:Configuration,appconf:AppParams, entTableName, entRegex).map(_.toEntRow)
    //F("ern", "idbrref", "name", "tradingstyle", "address1", "address2", "address3", "address4", "address5", "postcode", "legalstatus")
    //case class Ent(ern:String, idbrref:Option[String], name:Option[String], tradingstyle:Option[String], address1:Option[String], address2:Option[String], address3:Option[String], address4:Option[String], address5:Option[String], postcode:Option[String], legalstatus:Option[String])

    import spark.implicits._
    //existingEntRdd.toDF()
    val entDF: DataFrame = spark.createDataFrame(existingEntRdd,entRowSchema) //ENT record DF

    val luRows = updatedExistingLUs.map(_.toLuRow)
    val luDF = spark.createDataFrame(luRows,luRowSchema)

    val entTreeRows: DataFrame = entDF.join(luDF,"ern")

    val t = System.currentTimeMillis()
/*
    val existingEntHFileCells: RDD[(String, HFileCell)] = existingEntRdd.flatMap(record => record._2.map(cell => {
      val newTimePeriodKey = record._1.replace(s"~${appconf.PREVIOUS_TIME_PERIOD}", s"~${appconf.TIME_PERIOD}")
      (newTimePeriodKey,hfile.HFileCell(newTimePeriodKey,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY,cell.column, cell.value))
    }))


    val allEnts = existingEntHFileCells.union(newEnts)*/

    //save to hfile:
/*     allLinks.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
       .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],confs)

   allEnts.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
       .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],confs)*/

  }



  private def saveEnterpriseHFiles(confs: Configuration, appParams: AppParams, regex: String)(implicit spark: SparkSession) = {
    HBaseDao.readEnterprisesWithKeyFilter(confs, appParams, regex)
      .map(row => row.copy(row.key.replace(s"~${appParams.PREVIOUS_TIME_PERIOD}",s"~${appParams.TIME_PERIOD}")))
        .sortBy(row => s"${row.key}")
          .flatMap(_.toPutHFileEntries(appParams.HBASE_ENTERPRISE_COLUMN_FAMILY))
            .saveAsNewAPIHadoopFile(appParams.PATH_TO_ENTERPRISE_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], confs)
  }


  private def saveLinksHFiles(confs: Configuration, appParams: AppParams, regex: String)(implicit spark: SparkSession) = {
    HBaseDao.readLinksWithKeyFilter(confs, appParams, regex)
      .map(row => row.copy(row.key.replace(s"~${appParams.PREVIOUS_TIME_PERIOD}",s"~${appParams.TIME_PERIOD}")))
        .sortBy(row => s"${row.key}")
          .flatMap(_.toPutHFileEntries(appParams.HBASE_LINKS_COLUMN_FAMILY))
           .saveAsNewAPIHadoopFile(appParams.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], confs)
  }


}

