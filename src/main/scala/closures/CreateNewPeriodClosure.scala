package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import global.{AppParams, Configs}
import model.domain.{HFileRow, KVCell}
import model.hfile
import model.hfile.HFileCell
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.RddLogging
import spark.calculations.DataFrameHelper
import spark.extensions.sql._

import scala.util.Try




object CreateNewPeriodClosure extends WithConversionHelper with DataFrameHelper/* with RddLogging*/{


  type Cells = Iterable[KVCell[String, String]]
  type Record = (String, Cells)
  /**
    * copies data from Enterprise and Local Units tables into a new period data
    **/
  def createNewPeriodHfiles(confs: Configuration, appParams: AppParams)(implicit spark: SparkSession,connection:Connection): Unit = {
    //do enterprise. Local units to follow
    saveEnterpriseHFiles(confs, appParams, ".*ENT~"+{appParams.PREVIOUS_TIME_PERIOD}+"$") //.*(~201802)$ //.*(?!~ENT~)201802$
    saveLinksHFiles(confs, appParams, ".*(~ENT~"+{appParams.PREVIOUS_TIME_PERIOD}+")$")
  }


  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession,connection:Connection) = {

    val confs = Configs.conf

    val updatedConfs = appconf.copy(TIME_PERIOD=appconf.PREVIOUS_TIME_PERIOD)
    val parquetDF = spark.read.parquet(appconf.PATH_TO_PARQUET)
    // printDF("update parquet Schema:",parquetDF)


    val parquetRows: RDD[Row] = parquetDF.rdd
    val linksRecords: RDD[(String, HFileCell)] = parquetRows.flatMap(row => toLinksRefreshRecords(row,appconf))
    val updatesRdd: RDD[Record] = linksRecords.groupByKey().map(v => (v._1,v._2.map(kv => KVCell[String,String](kv.qualifier,kv.value))))//get LINKS updates from input parquet

    // printRdd("updatesRdd", updatesRdd,"Tuple (String,Iterable[KVCells])")


    //next 3 lines: select LU rows from hbase
    val linksTableName = s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
    val luRegex = ".*(ENT|LEU)~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val existingLuRdd: RDD[Record] = HBaseDao.readTableWithKeyFilter(confs,appconf, linksTableName, luRegex).map(row => (row.key.replace(s"~${appconf.PREVIOUS_TIME_PERIOD}",s"~${appconf.TIME_PERIOD}"),row.cells))

    // printRdd("existingLuRdd", updatesRdd,"Tuple (String,Iterable[KVCells])")

    val numOfPartitions = updatesRdd.getNumPartitions

    val joined: RDD[(String, (Option[Cells], Option[Cells]))] = updatesRdd.fullOuterJoin(existingLuRdd, numOfPartitions)

    // printRdd("links record updates rdd joined with existing LUs rdd",joined,"Tuple (String,Tuple(option[Iterable[KVCells]],option[Iterable[KVCells]])")

    /*
    * updatedExistingLUs - lu rows with all new data and with column link to parent enterprise copied from existing record("p_ENT")
    * */
    val updatedExistingLUs: RDD[HFileRow] = joined.collect { case (key, (Some(newCells), Some(oldCells))) => HFileRow(key,{newCells ++ oldCells.find(_.column=="p_ENT").map(ernCell => Seq(ernCell)).getOrElse(Seq.empty) })} // existing LUs updated with new cells

    // printRdd("updatedExistingLUs", updatedExistingLUs, "HFileRow")

    val newLUs: RDD[HFileRow] = joined.collect { case (key, (Some(newCells), None)) => HFileRow(key, newCells) }
    /*
    * existingLinksEnts contains links rows with ent row records with column links to child LU records
    * */
    val existingLinksEnts: RDD[HFileRow] = joined.collect { case (key, (None, Some(oldCells))) if(key.endsWith(s"ENT~${appconf.TIME_PERIOD}"))=> HFileRow(key, oldCells) }

    // printRdd("existingLinksEnts",existingLinksEnts,"HFileRow") //all strings: existingLinksEnts

    //new Records
    val newLuIds: RDD[(Long, Row)] = newLUs.filter(_.key.endsWith(s"~LEU~${appconf.TIME_PERIOD}")).collect{case HFileRow(key,_) if(key.endsWith(s"~${appconf.TIME_PERIOD}")) => (key.stripSuffix(s"~LEU~${appconf.TIME_PERIOD}").toLong,Row.empty)}

    val rowMapByKey: RDD[(Long, Row)] = parquetRows.map(row => (row.getLong("id").get, row))

    val joinedParquetRows: RDD[(Long, (Row, Option[Row]))] = newLuIds.leftOuterJoin(rowMapByKey,numOfPartitions)

    // printRdd("joinedParquetRows",joinedParquetRows,"(Long, (Row, Option[Row]))")

    val newLUParquetRows: RDD[Row] = joinedParquetRows.collect{  case (key,(oldRow,Some(newRow))) => {
      new GenericRowWithSchema(Array(
                newRow.getAs[String]("BusinessName"),
                newRow.getAs[String]("CompanyNo"),
                newRow.getAs[String]("EmploymentBands"),
                Try{newRow.getAs[String]("IndustryCode").toLong }.getOrElse(null),
                newRow.getAs[String]("LegalStatus"),
                newRow.getAs[Seq[String]]("PayeRefs"),
                newRow.getAs[String]("PostCode"),
                newRow.getAs[String]("TradingStatus"),
                newRow.getAs[String]("Turnover"),
                newRow.getAs[Long]("UPRN"),
                newRow.getAs[Seq[Long]]("VatRefs"),
                newRow.getAs[Long]("id")
      ),parquetRowSchema)


    } }


    // printRddOfRows("newLUParquetRows",newLUParquetRows)

    val newRowsDf: DataFrame = spark.createDataFrame(newLUParquetRows,parquetRowSchema)

    // printDF("newRowsDf",newRowsDf)

    val pathToPaye = appconf.PATH_TO_PAYE
    //// println(s"extracting paye file from path: $pathToPaye")

    val payeDf = spark.read.option("header", "true").csv(pathToPaye)
    // printDF("payeDf",payeDf)

    val newEntTree: RDD[hfile.Tables] = finalCalculations(newRowsDf, payeDf).rdd.map(row => toNewEnterpriseRecords(row,appconf))

    // printRdd("newEntTree",newEntTree,"hfile.Tables")

    val newEnts: RDD[(String, HFileCell)] =  newEntTree.flatMap(_.enterprises) //break into cells
    // printRdd("newEnts",newEnts,"(String, HFileCell)")



    val newLinks: RDD[(String, HFileCell)] =  newEntTree.flatMap(_.links) //break into cells
    // printRdd("newLinks",newLinks,"(String, HFileCell)")

    //existing records:
    val entRegex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val entTableName = s"${appconf.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appconf.HBASE_ENTERPRISE_TABLE_NAME}"
    val existingEntRdd: RDD[Row] = HBaseDao.readTableWithKeyFilter(confs:Configuration,appconf:AppParams, entTableName, entRegex).map(_.toEntRow)

    // printRddOfRows("existingEntRdd",existingEntRdd)
    val existingEntDF: DataFrame = spark.createDataFrame(existingEntRdd,entRowSchema) //ENT record to DF  --- no paye
    // printDF("existingEntDF",existingEntDF)


    val luRows: RDD[Row] = updatedExistingLUs.map(_.toLuRow)//.map(row => row.copy())
    // printRddOfRows("luRows",luRows)

    val ernWithPayesAndVats: RDD[Row] = luRows.collect{

      case row if(row.getStringSeq("PayeRefs").isDefined) => Row(
        row.getString("ern").get,
        row.getStringSeq("PayeRefs").get,
        row.getLongSeq("VatRefs").getOrElse(null)
      )

    }

    // printRddOfRows("ernWithPayesAndVats", ernWithPayesAndVats)

    val ernWithEmployeesdata: DataFrame = spark.createDataFrame(ernWithPayesAndVats,ernToEmployeesSchema) //DataFrame("ern":String, "payeRefs":Array[String],"VatRefs":Array[long])  DataFrame(ern, employees, jobs)
    // printDF("ernWithEmployeesdata",ernWithEmployeesdata)

    val payeDF: DataFrame = spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)
    // printDF("payeDF", payeDF)

    //// print("ernWithEmployeesdata>>NUM OF PARTITIONS: "+ernWithEmployeesdata.rdd.getNumPartitions)

    val ernPayeCalculatedDF: DataFrame = finalCalculationsEnt(ernWithEmployeesdata,payeDF)
    // printDF("ernPayeCalculatedDF", ernPayeCalculatedDF)

    val completeExistingEnts: RDD[Row] = existingEntDF.join(ernPayeCalculatedDF,Seq("ern"),"leftOuter").rdd.coalesce(numOfPartitions) //ready to go to rowToEnterprise(_,ern,_)
    // printRdd("completeExistingEnts", completeExistingEnts,"Row")


    /**
      * add new + existing enterprises and save to hfile
      * */
    val exsistingEntsCells: RDD[(String, HFileCell)] = completeExistingEnts.flatMap(row => rowToFullEnterprise(row,appconf))

    //val hfileRdd: RDD[(String, HFileCell)] = existingEntDF.rdd.flatMap(row => rowToEnterprise(row,appconf))

    val allEnts: RDD[(String, HFileCell)] = newEnts.union(exsistingEntsCells).coalesce(numOfPartitions)
     // printRdd("allEnts",allEnts,"(String, HFileCell)")



/**
  * add new + existing links and save to hfile
  * */

  val existingLusCells: RDD[(String, HFileCell)] = luRows.flatMap(r => rowToLegalUnitLinks("ubrn",r,appconf))

    // printRdd("existingLusCells",existingLusCells,"(String, HFileCell)")

  val allLus: RDD[(String, HFileCell)] = existingLusCells.union(newLinks).coalesce(numOfPartitions)

    // printRdd("allLus",allLus,"(String, HFileCell)")

  allLus.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

  allEnts.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)
 }

  private def saveEnterpriseHFiles(confs: Configuration, appParams: AppParams, regex: String)(implicit spark: SparkSession,connection:Connection) = {
    HBaseDao.readEnterprisesWithKeyFilter(confs, appParams, regex)
      .map(row => row.copy(row.key.replace(s"~${appParams.PREVIOUS_TIME_PERIOD}",s"~${appParams.TIME_PERIOD}")))
      .sortBy(row => s"${row.key}")
      .flatMap(_.toPutHFileEntries(appParams.HBASE_ENTERPRISE_COLUMN_FAMILY))
      .saveAsNewAPIHadoopFile(appParams.PATH_TO_ENTERPRISE_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], confs)
  }

  private def saveLinksHFiles(confs: Configuration, appParams: AppParams, regex: String)(implicit spark: SparkSession,connection:Connection) = {
    HBaseDao.readLinksWithKeyFilter(confs, appParams, regex)
      .map(row => row.copy(row.key.replace(s"~${appParams.PREVIOUS_TIME_PERIOD}",s"~${appParams.TIME_PERIOD}")))
      .sortBy(row => s"${row.key}")
      .flatMap(_.toPutHFileEntries(appParams.HBASE_LINKS_COLUMN_FAMILY))
      .saveAsNewAPIHadoopFile(appParams.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], confs)
  }


}

