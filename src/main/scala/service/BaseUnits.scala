package service

import dao.DaoUtils._
import dao.hbase.{HBaseDao, HFilePartitioner, HFileUtils}
import model.{HFileCell, Schemas}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import util.configuration.AssemblerConfiguration
import util.configuration.AssemblerHBaseConfiguration._

trait BaseUnits extends HFileUtils with Serializable {

  /**
    * Fields:
    * id,BusinessName, UPRN, PostCode,IndustryCode,LegalStatus,
    * TradingStatus, Turnover, EmploymentBands, PayeRefs, VatRefs, CompanyNo
    **/
  def getIncomingBiData()(implicit spark: SparkSession): DataFrame = {
    val parquetDF = spark.read.parquet(AssemblerConfiguration.PathToParquet)
    val rows = parquetDF.castAllToString.rdd.map { row => {

      Row(
        null,
        row.getAs[String]("id"),
        row.getAs[String]("BusinessName"),
        row.getAs[String]("IndustryCode"),
        row.getAs[String]("LegalStatus"),
        row.getValueOrEmptyStr("Address1"),
        row.getAs[String]("Address2"),
        row.getAs[String]("Address3"),
        row.getAs[String]("Address4"),
        row.getAs[String]("Address5"),
        row.getStringValueOrDefault("PostCode", AssemblerConfiguration.DefaultPostCode),
        row.getAs[String]("TradingStatus"),
        row.getAs[String]("Turnover"),
        row.getAs[String]("UPRN"),
        row.getAs[String]("CompanyNo"),
        row.getAs[Seq[String]]("PayeRefs"),
        row.getAs[Seq[String]]("VatRefs")
      )
    }
    }
    spark.createDataFrame(rows, Schemas.biWithErnSchema)

  }

  def getAllLUs(joinedLUs: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val rows = joinedLUs.rdd.map { row => {
      val ern = if (row.isNull("ern")) generateErn(row) else row.getAs[String]("ern")

      Row(
        ern,
        row.getAs[String]("ubrn"),
        row.getAs[String]("name"),
        row.getAs[String]("industry_code"),
        row.getAs[String]("legal_status"),
        row.getValueOrEmptyStr("address1"),
        row.getAs[String]("address2"),
        row.getAs[String]("address3"),
        row.getAs[String]("address4"),
        row.getAs[String]("address5"),
        row.getValueOrEmptyStr("postcode"),
        row.getAs[String]("trading_status"),
        row.getAs[String]("turnover"),
        row.getAs[String]("uprn"),
        row.getAs[String]("crn"),
        row.getAs[Seq[String]]("payerefs"),
        row.getAs[Seq[String]]("vatrefs")
      )
    }
    }
    spark.createDataFrame(rows, Schemas.biWithErnSchema)
  }

  def createNewRus(ents: DataFrame)(implicit spark: SparkSession): DataFrame = {

    spark.createDataFrame(
      ents.rdd.map(row => Row(
        generateRurn(row),
        row.getAs[String]("ern"),
        row.getAs[String]("name"),
        row.getValueOrNull("entref"),
        row.getValueOrNull("ruref"),
        row.getValueOrNull("trading_style"),
        row.getAs[String]("legal_status"),
        row.getAs[String]("address1"),
        row.getValueOrNull("address2"),
        row.getValueOrNull("address3"),
        row.getValueOrNull("address4"),
        row.getValueOrNull("address5"),
        row.getValueOrEmptyStr("postcode"),
        row.getValueOrEmptyStr("sic07"),
        row.getValueOrEmptyStr("paye_empees"),
        row.getValueOrEmptyStr("turnover"),
        generatePrn(row),
        row.getValueOrEmptyStr("region"),
        row.getValueOrEmptyStr("employment")
      )), Schemas.ruRowSchema)
  }

  /**
    * Creates new Enterprises from new LEUs with calculations
    * schema: completeNewEntSchema
    * ern, entref, name, address1, postcode, sic07, legal_status,
    * AND calculations:
    * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
    **/
  def createNewEntsWithCalculations(newLEUsCalculatedDF: DataFrame)(implicit spark: SparkSession): DataFrame = spark.createDataFrame(

    newLEUsCalculatedDF.rdd.map(row => Row(
      row.getAs[String]("ern"),
      generatePrn(row),
      row.getValueOrNull("entref"),
      row.getAs[String]("name"),
      null, //trading_style
      row.getValueOrEmptyStr("address1"),
      row.getAs[String]("address2"),
      row.getAs[String]("address3"),
      row.getAs[String]("address4"),
      row.getAs[String]("address5"),
      row.getAs[String]("postcode"),
      row.getValueOrEmptyStr("region"),
      row.getValueOrEmptyStr("industry_code"),
      row.getAs[String]("legal_status"),
      row.getValueOrNull("paye_empees"),
      row.getValueOrNull("paye_jobs"),
      row.getValueOrNull("cntd_turnover"),
      row.getValueOrNull("app_turnover"),
      row.getValueOrNull("std_turnover"),
      row.getValueOrNull("grp_turnover"),
      row.getValueOrNull("ent_turnover"),
      row.getStringOption("working_props").getOrElse("0"),
      row.getStringOption("employment").getOrElse("0")
    )), Schemas.completeEntSchema)

  def createNewLous(rus: DataFrame)(implicit spark: SparkSession): DataFrame = {

    spark.createDataFrame(
      rus.rdd.map(row => Row(
        generateLurn(row),
        row.getValueOrNull("luref"), //will not be present
        row.getAs[String]("ern"),
        generatePrn(row),
        row.getAs[String]("rurn"),
        row.getValueOrNull("ruref"),
        row.getAs[String]("name"),
        row.getValueOrNull("entref"),
        row.getValueOrNull("trading_style"),
        row.getAs[String]("address1"),
        row.getValueOrNull("address2"),
        row.getValueOrNull("address3"),
        row.getValueOrNull("address4"),
        row.getValueOrNull("address5"),
        row.getValueOrEmptyStr("postcode"),
        row.getValueOrEmptyStr("region"),
        row.getValueOrEmptyStr("sic07"),
        row.getValueOrEmptyStr("employees"),
        row.getValueOrEmptyStr("employment")
      )), Schemas.louRowSchema)
  }

  def getExistingRusDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val ruHFileRowRdd: RDD[Row] = HBaseDao.readTable(confs, HBaseDao.rusTableName).map(_.toRuRow)
    spark.createDataFrame(ruHFileRowRdd, Schemas.ruRowSchema)
  }

  def getExistingLousDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val louHFileRowRdd: RDD[Row] = HBaseDao.readTable(confs, HBaseDao.lousTableName).map(_.toLouRow)
    spark.createDataFrame(louHFileRowRdd, Schemas.louRowSchema)
  }

  def getExistingEntsDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val entHFileRowRdd: RDD[Row] = HBaseDao.readTable(confs, HBaseDao.entsTableName).map(_.toEntRow)
    spark.createDataFrame(entHFileRowRdd, Schemas.entRowSchema)
  }

  /**
    * returns existing LEU~ links DF
    * fields:
    * ubrn, ern, CompanyNo, PayeRefs, VatRefs
    **/
  def getExistingLinksLeusDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val leuHFileRowRdd: RDD[Row] = HBaseDao.readTable(confs, HBaseDao.linksTableName).map(_.toLeuLinksRow)
    spark.createDataFrame(leuHFileRowRdd, Schemas.linksLeuRowSchema)
  }

  def getExistingLeusDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    val leuHFileRowRdd: RDD[Row] = HBaseDao.readTable(confs, HBaseDao.leusTableName).map(_.toLeuRow)
    spark.createDataFrame(leuHFileRowRdd, Schemas.leuRowSchema)
  }

  def saveLinks(louDF: DataFrame, ruDF: DataFrame, leuDF: DataFrame)
               (implicit spark: SparkSession, connection: Connection): Unit = {
    import spark.implicits._

    val tableName = TableName.valueOf(HBaseDao.linksTableName)
    val regionLocator = connection.getRegionLocator(tableName)
    val partitioner = HFilePartitioner(connection.getConfiguration, regionLocator.getStartKeys, 1)

    val rusLinks: RDD[(String, HFileCell)] = ruDF.map(row => ruToLinks(row)).flatMap(identity).rdd
    val lousLinks: RDD[(String, HFileCell)] = louDF.map(row => louToLinks(row)).flatMap(identity).rdd
    val restOfLinks: RDD[(String, HFileCell)] = leuDF.map(row => leuToLinks(row)).flatMap(identity).rdd

    val allLinks: RDD[((String, String), HFileCell)] = lousLinks.union(rusLinks)
      .union(restOfLinks)
      .filter(_._2.value != null)
      .map(entry => ((entry._1, entry._2.qualifier), entry._2))
      .repartitionAndSortWithinPartitions(partitioner)
    allLinks.map(rec => (new ImmutableBytesWritable(rec._1._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(AssemblerConfiguration.PathToLinksHfile, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
  }

  def saveEnts(entsDF: DataFrame)(implicit spark: SparkSession): Unit = {
    val rep = entsDF.coalesce(1)
    val hfileCells: RDD[(String, HFileCell)] = getEntHFileCells(rep)
    val a: RDD[(String, HFileCell)] = hfileCells.filter(_._2.value != null)
    val b: RDD[(String, HFileCell)] = a.sortBy(t => t._2.key.toString + t._2.qualifier.toString)
    val c: RDD[(ImmutableBytesWritable, KeyValue)] = b.map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))

    c.saveAsNewAPIHadoopFile(AssemblerConfiguration.PathToEnterpriseHFile,
      classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
  }

  def saveLous(lousDF: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val rep = lousDF.coalesce(1)
    val hfileCells = rep.map(row => rowToLocalUnit(row)).flatMap(identity).rdd
    hfileCells.filter(_._2.value != null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(AssemblerConfiguration.PathToLocalUnitsHFile, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
  }

  def saveLeus(leusDF: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val rep = leusDF.coalesce(1)
    val hfileCells = rep.map(row => rowToLegalUnit(row)).flatMap(identity).rdd
    hfileCells.filter(_._2.value != null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(AssemblerConfiguration.PathToLegalUnitsHFile, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
  }

  def saveRus(rusDF: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val rep = rusDF.coalesce(1)
    val hfileCells = rep.map(row => rowToReportingUnit(row)).flatMap(identity).rdd
    hfileCells.filter(_._2.value != null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(AssemblerConfiguration.PathToReportingUnitsHFile, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
  }

  private def getEntHFileCells(entsDF: DataFrame)(implicit spark: SparkSession): RDD[(String, HFileCell)] = {
    import spark.implicits._
    val res: RDD[(String, HFileCell)] = entsDF.map(row => rowToEnt(row)).flatMap(identity).rdd
    res
  }

}
