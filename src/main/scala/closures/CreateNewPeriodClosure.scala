package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.{UnitRowConverter, WithConversionHelper}
import global.{AppParams, Configs}
import model.domain.{HFileRow, KVCell}
import model.hfile
import model.hfile.{HFileCell, Tables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.RddLogging
import spark.calculations.{AdminDataCalculator, DataFrameHelper}
import spark.extensions.sql._
import org.apache.spark.sql.functions._

import scala.util.Try


trait CreateNewPeriodClosure extends WithConversionHelper with UnitRowConverter with DataFrameHelper with AdminDataCalculator with RddLogging with Serializable{

  val hbaseDao: HBaseDao = HBaseDao

  type Cells = Iterable[KVCell[String, String]]
  type Record = (String, Cells)


  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession):Unit = {
    val confs = Configs.conf

    val updatedConfs = appconf.copy(TIME_PERIOD=appconf.PREVIOUS_TIME_PERIOD)
    val parquetDF = spark.read.parquet(appconf.PATH_TO_PARQUET)
    //printDF("update parquet Schema:",parquetDF)


    //val parquetRows: RDD[Row] = parquetDF.rdd
    // printRddOfRows("parquetRows",parquetRows)

    val castedToStringParquetDF = parquetDF.castAllToString
    //printDF("castedToStringParquetDF:",castedToStringParquetDF)

    val linksRecords: RDD[(String, HFileCell)] = castedToStringParquetDF.rdd.flatMap(row => toLinksRefreshRecords(row,appconf))
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
    joined.cache()
 // printRdd("links record updates rdd joined with existing LUs rdd",joined,"Tuple (String,Tuple(option[Iterable[KVCells]],option[Iterable[KVCells]])")

    /*
    * updatedExistingLUs - lu rows with all new data and with column link to parent enterprise copied from existing record("p_ENT")
    * */
    val updatedExistingLUs: RDD[HFileRow] = joined.collect {
      case (key, (Some(newCells), Some(oldCells))) => HFileRow(key,{newCells ++ oldCells.find(_.column=="p_ENT").map(ernCell => Seq(ernCell)).getOrElse(Seq.empty) })

    } // existing LUs updated with new cells

 // printRdd("updatedExistingLUs", updatedExistingLUs, "HFileRow")

    val newLUs: RDD[HFileRow] = joined.collect { case (key, (Some(newCells), None)) => HFileRow(key, newCells) }
    //printCount(newLUs,"new LUs count: ")
    /*
    * existingLinksEnts contains links rows with ent row records with column links to child LU records
    * */
    val existingLinksEnts: RDD[HFileRow] = joined.collect { case (key, (None, Some(oldCells))) if(key.endsWith(s"ENT~${appconf.TIME_PERIOD}"))=> HFileRow(key, oldCells) }
 // printCount(existingLinksEnts,"existing Enterprises: ")
    //printRdd("existingLinksEnts",existingLinksEnts,"HFileRow") //all strings: existingLinksEnts

    val preCalculatedLUs: RDD[Row] = NewPeriodWithCalculations.getAllLUs(joined)
    //printRddOfRows("preCalculatedLUs", preCalculatedLUs)
    val preCalculatedDF = spark.createDataFrame(preCalculatedLUs,preCalculateDfSchema)

    preCalculatedDF.cache()
    val calculatedDF = AdminDataCalculator.calculate(preCalculatedDF,appconf).cache()

    //calculatedDF.show()
    //calculatedDF.printSchema()
    preCalculatedDF.unpersist()
    joined.unpersist()

    //new Records
    val newLuIds: RDD[(String, Row)] = newLUs.filter(_.key.endsWith(s"~LEU~${appconf.TIME_PERIOD}")).collect{case HFileRow(key,_) if(key.endsWith(s"~${appconf.TIME_PERIOD}")) => (key.stripSuffix(s"~LEU~${appconf.TIME_PERIOD}"),Row.empty)}

    val rowMapByKey: RDD[(String, Row)] = castedToStringParquetDF.rdd.map(row => (row.getString("id").get, row))

    val joinedParquetRows: RDD[(String, (Row, Option[Row]))] = newLuIds.leftOuterJoin(rowMapByKey,numOfPartitions)



    val newLUParquetRows: RDD[Row] = joinedParquetRows.collect{  case (key,(oldRow,Some(newRow))) => {
      new GenericRowWithSchema(Array(
                newRow.getAs[String]("BusinessName"),
                newRow.getAs[String]("CompanyNo"),
                Try{newRow.getAs[String]("IndustryCode")}.getOrElse(""),
                newRow.getAs[String]("LegalStatus"),
                newRow.getAs[Seq[String]]("PayeRefs"),
                newRow.getAs[String]("PostCode"),
                newRow.getAs[String]("TradingStatus"),
                newRow.getAs[String]("Turnover"),
                newRow.getAs[String]("UPRN"),
                newRow.getAs[Seq[String]]("VatRefs"),
                newRow.getAs[String]("id")
      ),parquetRowSchema)


    } }


    val newRowsDf: DataFrame = spark.createDataFrame(newLUParquetRows,parquetRowSchema)



    val pathToPaye = appconf.PATH_TO_PAYE
    val pathToVat = appconf.PATH_TO_VAT

    val payeDf = spark.read.option("header", "true").csv(pathToPaye)


    val vatDf = spark.read.option("header", "true").csv(pathToVat)



    /**
      * new ent row AND new LINKS row
      * */
    val newEntWithLinks: DataFrame = newRowsDf.map(row => biRowToEnt(row)).toDF()

     newEntWithLinks.cache()
     val newEntDF = newEntWithLinks.select(
                                          "ern", "entref", "name", "trading_style",
                                          "address1", "address2", "address3", "address4", "address5",
                                          "postcode", "sic07", "legal_status"
                                         )


    val newLinksDF =  newEntWithLinks.select("ubrn", "CompanyNo", "PayeRefs", "VatRefs")

    val existingEntDF: DataFrame = getExistingEntsDF(appconf,confs)

    val allEnts = newEntDF.union(existingEntDF)
    val calculatedEnts = allEnts.join(calculatedDF, col("ern"), "left_outer")

    val luRows: RDD[Row] = updatedExistingLUs.map(_.toLuRow)



    /**
      * add new + existing enterprises and save to hfile
      * */
    val allCompleteEnts: RDD[(String, HFileCell)] = {
      val ss = calculatedEnts.sparkSession
      import ss.implicits._
      calculatedEnts.map(row => rowToFullEnterprise(row,appconf)).flatMap(identity).rdd
    }

   val entsWithMissingLous: RDD[Row] = getEntsWithMissingLous(allEnts,appconf,confs)

   val missingLousData: RDD[Tables]= entsWithMissingLous.map(row => entToLocalUnits(row,appconf))
   missingLousData.cache()


   val missingLous: RDD[(String, HFileCell)] =  missingLousData.flatMap(_.localUnits)


   /**
   * add new + existing links and save to hfile
   * */

  val existingEntLinkRefs: RDD[(String, HFileCell)] = existingLinksEnts.flatMap(hfrow => hfrow.toHFileCellRow(appconf.HBASE_LINKS_COLUMN_FAMILY))
  val existingLousCells: RDD[(String, HFileCell)] = existingLous.flatMap(row => row._2.map(cell => (row._1,HFileCell(row._1, appconf.HBASE_LINKS_COLUMN_FAMILY, cell.column, cell.value))))
  val existingLinkCells: RDD[(String, HFileCell)] = luRows.flatMap(r => rowToLegalUnitLinks("ubrn",r,appconf)).union(existingEntLinkRefs).union(existingLousCells)

  //printRdd("existingLusCells",existingLusCells,"(String, HFileCell)")

  val newLinks = newLinksDF

  val allLinks: RDD[(String, HFileCell)] = existingLinkCells.union(newLinks).union(missingLousLinks).coalesce(numOfPartitions)

  //printRdd("allLus",allLus,"(String, HFileCell)")



  allLinks.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)



    //printRdd("allCompleteEnts",allCompleteEnts,"(String, HFileCell)")

  allCompleteEnts.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)


   val lous: RDD[(String, HFileCell)] =  newEntTree.flatMap(_.localUnits).union(missingLous)

   saveAllLocalUnits(lous,appconf,confs)
   missingLousData.unpersist()
   completeExistingEnts.unpersist()
   newEntTree.unpersist()

  }

  def getExistingEntsDF(appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val entRegex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val entTableName = s"${appconf.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appconf.HBASE_ENTERPRISE_TABLE_NAME}"
    val entHFileRowRdd: RDD[HFileRow] = hbaseDao.readTableWithKeyFilter(confs,appconf, entTableName, entRegex)
    //printRdd("entHFileRowRdd",entHFileRowRdd,"HFileRow")
    val existingEntRdd: RDD[Row] = entHFileRowRdd.map(_.toEntRow)
    //printRddOfRows("existingEntRdd",existingEntRdd)
    spark.createDataFrame(existingEntRdd,entRowSchema) //ENT record to DF  --- no paye
  }

  def saveAllLocalUnits(newLous:RDD[(String, HFileCell)],appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val existingLous = getExistingLocalUnits(appconf, confs)

    val allLous = existingLous.union(newLous)
    allLous.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_LOCALUNITS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

  }

  def getExistingLocalUnits(appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    //next 3 lines: select LOU rows from hbase
    val localUnitsTableName = s"${appconf.HBASE_LOCALUNITS_TABLE_NAMESPACE}:${appconf.HBASE_LOCALUNITS_TABLE_NAME}"
    val regex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"~.*"
    val existingLouRdd: RDD[Record] = hbaseDao.readTableWithKeyFilter(confs,appconf, localUnitsTableName, regex).map(row => (row.key.replace(s"~${appconf.PREVIOUS_TIME_PERIOD}",s"~${appconf.TIME_PERIOD}"),row.cells))

    val louRowCells: RDD[(String, HFileCell)] = existingLouRdd.flatMap(rec => rec._2.map(cell => (rec._1,HFileCell(rec._1,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY,cell.column,cell.value))))
    louRowCells

  }

  def getEntsWithMissingLous(completeExistingEnts: DataFrame,appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {


    val localUnitsTableName = s"${appconf.HBASE_LOCALUNITS_TABLE_NAMESPACE}:${appconf.HBASE_LOCALUNITS_TABLE_NAME}"
    val regex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"~.*"
    val lous: RDD[HFileRow] = hbaseDao.readTableWithKeyFilter(confs,appconf, localUnitsTableName, regex)


    val louRows: RDD[Row] = lous.map(hfileRow => {
      new GenericRowWithSchema(Array(
          hfileRow.getCellValue("ern"),
          hfileRow.getCellValue("lurn")
          ),louIdsSchema)
    })
    //printRddOfRows("louRows", louRows)


    val existingLousDF: DataFrame = spark.createDataFrame(louRows,louIdsSchema)
    //printDF("existingLousDF",existingLousDF)
    existingLousDF.createOrReplaceTempView("LOUS")
    completeExistingEnts.createOrReplaceTempView("ENTS")
    val sqlDF: DataFrame = spark.sql("SELECT * FROM ENTS where ENTS.ern NOT IN(SELECT ern FROM LOUS)")
    //printDF("sqlDF",sqlDF)
    val rdd: RDD[Row] = sqlDF.rdd
    //printRddOfRows("sqlDF", rdd)
    rdd

  }

  private def tupleToRow(tuple:(String,String)) = {
    val (ern,lurn) = tuple
    new GenericRowWithSchema(Array(
      ern,lurn
    ),louIdsSchema)
  }

}

object CreateNewPeriodClosure extends CreateNewPeriodClosure

