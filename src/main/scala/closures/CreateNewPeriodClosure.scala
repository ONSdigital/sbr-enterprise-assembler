package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import global.{AppParams, Configs}
import model.domain.{HFileRow, KVCell}
import model.hfile
import model.hfile.HFileCell
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.RddLogging
import spark.calculations.DataFrameHelper
import spark.extensions.sql._

import scala.util.Try




trait CreateNewPeriodClosure extends Serializable with WithConversionHelper with DataFrameHelper/* with RddLogging*/{

  val hbaseDao: HBaseDao = HBaseDao

  type Cells = Iterable[KVCell[String, String]]
  type Record = (String, Cells)


  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession):Unit = {

    val confs = Configs.conf

    val updatedConfs = appconf.copy(TIME_PERIOD=appconf.PREVIOUS_TIME_PERIOD)
    val parquetDF = spark.read.parquet(appconf.PATH_TO_PARQUET)
    // printDF("update parquet Schema:",parquetDF)


    val parquetRows: RDD[Row] = parquetDF.rdd
    // printRddOfRows("parquetRows",parquetRows)
    val linksRecords: RDD[(String, HFileCell)] = parquetRows.flatMap(row => toLinksRefreshRecords(row,appconf))
    val updatesRdd: RDD[Record] = linksRecords.groupByKey().map(v => (v._1,v._2.map(kv => KVCell[String,String](kv.qualifier,kv.value))))//get LINKS updates from input parquet

 // printRdd("updatesRdd", updatesRdd,"Tuple (String,Iterable[KVCells])")


    //next 3 lines: select LU rows from hbase
    val linksTableName = s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
    val luRegex = ".*(ENT|LEU)~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val existingLuRdd: RDD[Record] = hbaseDao.readTableWithKeyFilter(confs,appconf, linksTableName, luRegex).map(row => (row.key.replace(s"~${appconf.PREVIOUS_TIME_PERIOD}",s"~${appconf.TIME_PERIOD}"),row.cells))


    val louRegex = ".*~LOU~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val existingLous: RDD[Record]  = hbaseDao.readTableWithKeyFilter(confs,appconf, linksTableName, louRegex).map(row => (row.key.replace(s"~${appconf.PREVIOUS_TIME_PERIOD}",s"~${appconf.TIME_PERIOD}"),row.cells))

    //printRdd("existingLous", existingLous,"Tuple (String,Iterable[KVCells])")

    val numOfPartitions = updatesRdd.getNumPartitions

    val joined: RDD[(String, (Option[Cells], Option[Cells]))] = updatesRdd.fullOuterJoin(existingLuRdd, numOfPartitions)

 // printRdd("links record updates rdd joined with existing LUs rdd",joined,"Tuple (String,Tuple(option[Iterable[KVCells]],option[Iterable[KVCells]])")

    /*
    * updatedExistingLUs - lu rows with all new data and with column link to parent enterprise copied from existing record("p_ENT")
    * */
    val updatedExistingLUs: RDD[HFileRow] = joined.collect {
      case (key, (Some(newCells), Some(oldCells))) => HFileRow(key,{newCells ++ oldCells.find(_.column=="p_ENT").map(ernCell => Seq(ernCell)).getOrElse(Seq.empty) })

    } // existing LUs updated with new cells

 // printRdd("updatedExistingLUs", updatedExistingLUs, "HFileRow")

    val newLUs: RDD[HFileRow] = joined.collect { case (key, (Some(newCells), None)) => HFileRow(key, newCells) }
 // printCount(newLUs,"new LUs count: ")
    /*
    * existingLinksEnts contains links rows with ent row records with column links to child LU records
    * */
    val existingLinksEnts: RDD[HFileRow] = joined.collect { case (key, (None, Some(oldCells))) if(key.endsWith(s"ENT~${appconf.TIME_PERIOD}"))=> HFileRow(key, oldCells) }
 // printCount(existingLinksEnts,"existing Enterprises: ")
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
                Try{newRow.getAs[Long]("IndustryCode").toString}.getOrElse(""),
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

    //printDF("newRowsDf",newRowsDf)

    val pathToPaye = appconf.PATH_TO_PAYE
    //// println(s"extracting paye file from path: $pathToPaye")

    val pathToVat = appconf.PATH_TO_VAT

    val payeDf = spark.read.option("header", "true").csv(pathToPaye)
    // printDF("payeDf",payeDf)

    val vatDf = spark.read.option("header", "true").csv(pathToVat)

    val newEntTree: RDD[hfile.Tables] = adminCalculations(newRowsDf, payeDf, vatDf).rdd.map(row => toNewEnterpriseRecordsWithLou(row,appconf))

    // println("PARTITIONS OF newEntTree: "+newEntTree.getNumPartitions)

    // printRdd("newEntTree",newEntTree,"hfile.Tables")

     newEntTree.cache()

    val newEnts: RDD[(String, HFileCell)] =  newEntTree.flatMap(_.enterprises) //break into cells
    //printRdd("newEnts",newEnts,"(String, HFileCell)")



    val newLinks: RDD[(String, HFileCell)] =  newEntTree.flatMap(_.links) //break into cells
 // printRdd("newLinks",newLinks,"(String, HFileCell)")
    //newEntTree.unpersist()
    //existing records:
    val entRegex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val entTableName = s"${appconf.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appconf.HBASE_ENTERPRISE_TABLE_NAME}"
    val existingEntRdd: RDD[Row] = hbaseDao.readTableWithKeyFilter(confs,appconf, entTableName, entRegex).map(_.toEntRow)

    // printRddOfRows("existingEntRdd",existingEntRdd)
    val existingEntDF: DataFrame = spark.createDataFrame(existingEntRdd,entRowSchema) //ENT record to DF  --- no paye
    //printDF("existingEntDF",existingEntDF)


    val luRows: RDD[Row] = updatedExistingLUs.map(_.toLuRow)//.map(row => row.copy())
 // printRddOfRows("luRows",luRows)

    val ernWithPayesAndVats: RDD[Row] = luRows.collect{

      case row if(row.getStringSeq("PayeRefs").isDefined || row.getLongSeq("VatRefs").isDefined) => Row(
        row.getString("ern").get,
        row.getStringSeq("PayeRefs").getOrElse(null),
        row.getLongSeq("VatRefs").getOrElse(null)
      )
    }

    // printRddOfRows("ernWithPayesAndVats", ernWithPayesAndVats)

    val ernWithEmployeesdata: DataFrame = spark.createDataFrame(ernWithPayesAndVats,ernToEmployeesSchema) //DataFrame("ern":String, "payeRefs":Array[String],"VatRefs":Array[long])  DataFrame(ern, employees, jobs)
    //printDF("ernWithEmployeesdata",ernWithEmployeesdata)

    val payeDF: DataFrame = spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)
    //printDF("payeDF", payeDF)

    val vatDF: DataFrame = spark.read.option("header", "true").csv(appconf.PATH_TO_VAT)
    //printDF("vatDF", vatDF)
    //// print("ernWithEmployeesdata>>NUM OF PARTITIONS: "+ernWithEmployeesdata.rdd.getNumPartitions)

    val ernPayeCalculatedDF: DataFrame = adminCalculationsEnt(ernWithEmployeesdata,payeDF, vatDF,"ern")
    //printDF("ernPayeCalculatedDF", ernPayeCalculatedDF)
    val completeExistingEnts: DataFrame = existingEntDF.join(ernPayeCalculatedDF,Seq("ern"),"leftOuter")//.rdd.coalesce(numOfPartitions) //ready to go to rowToEnterprise(_,ern,_)
    completeExistingEnts.cache()
      //printDF("existingEntDF", existingEntDF)
      //printDF("completeExistingEnts", completeExistingEnts)


    /**
      * add new + existing enterprises and save to hfile
      * */
    val existingEntsCells: RDD[(String, HFileCell)] = {
      val ss = completeExistingEnts.sparkSession
      import ss.implicits._
      completeExistingEnts.map(row => rowToFullEnterprise(row,appconf)).flatMap(identity).rdd
    }
    //printRdd("existingEntsCells", existingEntsCells, "(String,HFileCell)")
    val allEnts: RDD[(String, HFileCell)] = newEnts.union(existingEntsCells)
    //printRdd("allEnts",allEnts,"(String, HFileCell)")



   /**
   * add new + existing links and save to hfile
   * */

  val existingEntLinkRefs: RDD[(String, HFileCell)] = existingLinksEnts.flatMap(hfrow => hfrow.toHFileCellRow(appconf.HBASE_LINKS_COLUMN_FAMILY))
  val existingLousCells: RDD[(String, HFileCell)] = existingLous.flatMap(row => row._2.map(cell => (row._1,HFileCell(row._1, appconf.HBASE_LINKS_COLUMN_FAMILY, cell.column, cell.value))))
  val existingLusCells: RDD[(String, HFileCell)] = luRows.flatMap(r => rowToLegalUnitLinks("ubrn",r,appconf)).union(existingEntLinkRefs).union(existingLousCells)

  //printRdd("existingLusCells",existingLusCells,"(String, HFileCell)")

  val allLus: RDD[(String, HFileCell)] = existingLusCells.union(newLinks).coalesce(numOfPartitions)

  //printRdd("allLus",allLus,"(String, HFileCell)")



  allLus.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    //printRdd("allEnts",allEnts,"(String, HFileCell)")

  allEnts.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)


   val lous =  newEntTree.flatMap(_.localUnits)

   saveAllLocalUnits(lous,appconf,confs)

  completeExistingEnts.unpersist()
  newEntTree.unpersist()

  }

  def saveAllLocalUnits(newLous:RDD[(String, HFileCell)],appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val existingLous = getExistingLocalUnits(appconf, confs)

    val allLous = existingLous.union(newLous)
    allLous.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LOCALUNITS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

  }

  def getExistingLocalUnits(appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    //val updatedConfs = appconf.copy(TIME_PERIOD=appconf.PREVIOUS_TIME_PERIOD)
    //next 3 lines: select LOU rows from hbase
    val localUnitsTableName = s"${appconf.HBASE_LOCALUNITS_TABLE_NAMESPACE}:${appconf.HBASE_LOCALUNITS_TABLE_NAME}"
    val regex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"~.*"
    val existingLouRdd: RDD[Record] = hbaseDao.readTableWithKeyFilter(confs,appconf, localUnitsTableName, regex).map(row => (row.key.replace(s"~${appconf.PREVIOUS_TIME_PERIOD}",s"~${appconf.TIME_PERIOD}"),row.cells))

    val louRowCells: RDD[(String, HFileCell)] = existingLouRdd.flatMap(rec => rec._2.map(cell => (rec._1,HFileCell(rec._1,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY,cell.column,cell.value))))
    louRowCells

  }

}

object CreateNewPeriodClosure extends CreateNewPeriodClosure

