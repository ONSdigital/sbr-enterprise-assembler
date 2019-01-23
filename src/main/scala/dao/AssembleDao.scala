package dao

import dao.DaoUtils._
import dao.hbase.{HBaseDao, HFilePartitioner, HFileUtils}
import model.{HFileCell, Schemas}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import util.configuration.AssemblerConfiguration
import util.configuration.AssemblerHBaseConfiguration.hbaseConfiguration

class AssembleDao extends HFileUtils {

  @transient private lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")

  /**
    * Fields:
    * id,BusinessName, UPRN, PostCode,IndustryCode,LegalStatus,
    * TradingStatus, Turnover, EmploymentBands, PayeRefs, VatRefs, CompanyNo
    **/
  def getIncomingBiData()(implicit spark: SparkSession): DataFrame = {

    log.debug("Start getIncomingBiData")

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

    log.debug("End getIncomingBiData")

    spark.createDataFrame(rows, Schemas.biWithErnSchema)

  }

  def getAllLUs(joinedLUs: DataFrame)(implicit spark: SparkSession): DataFrame = {

    log.debug("Start getAllLUs")

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

    log.debug("End getAllLUs")

    spark.createDataFrame(rows, Schemas.biWithErnSchema)
  }

  /**
    * Creates new Enterprises from new LEUs with calculations
    * schema: completeNewEntSchema
    * ern, entref, name, address1, postcode, sic07, legal_status,
    * AND calculations:
    * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
    **/
  def createNewEntsWithCalculations(newLEUsCalculatedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    log.debug("Start createNewEntsWithCalculations")

    val df = spark.createDataFrame(

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

    log.debug("End createNewEntsWithCalculations")

    df
  }

  def getExistingEntsDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    log.debug("Start getExistingEntsDF")
println(HBaseDao.readTable(confs, HBaseDao.entsTableName))
    val entHFileRowRdd: RDD[Row] = HBaseDao.readTable(confs, HBaseDao.entsTableName).map(_.toEntRow)
    val df = spark.createDataFrame(entHFileRowRdd, Schemas.entRowSchema)
    df.show()
    log.debug("End getExistingEntsDF")
    df
  }

  /**
    * returns existing LEU~ links DF
    * fields:
    * ubrn, ern, CompanyNo, PayeRefs, VatRefs
    **/
  def getExistingLinksLeusDF(confs: Configuration)(implicit spark: SparkSession): DataFrame = {
    log.debug("Start getExistingLinksLeusDF")

    val leuHFileRowRdd: RDD[Row] = HBaseDao.readTable(confs, HBaseDao.linksTableName).map(_.toLeuLinksRow)
    val df = spark.createDataFrame(leuHFileRowRdd, Schemas.linksLeuRowSchema)
    log.debug("End getExistingLinksLeusDF")
    df
  }

  def saveAdminData(AdminDataDF: DataFrame)(implicit spark: SparkSession): Unit = {
    log.debug("Start saveAdminData")

    import spark.implicits._
    val hfileCells = AdminDataDF.map(row => rowToAdminData(row)).flatMap(identity).rdd

    hfileCells.filter(_._2.value != null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(AssemblerConfiguration.PathToAdminDataHFile, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
    log.debug("End saveLeus")
  }

  def saveRegion(RegionDF: DataFrame)(implicit spark: SparkSession): Unit = {
    log.debug("Start saveAdminData")

    import spark.implicits._
    val hfileCells = RegionDF.map(row => rowToRegion(row)).flatMap(identity).rdd

    hfileCells.filter(_._2.value != null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(AssemblerConfiguration.PathToRegionHFile, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
    log.debug("End saveLeus")
  }

  def saveEmployment(EmploymentDF: DataFrame)(implicit spark: SparkSession): Unit = {
    log.debug("Start saveAdminData")

    import spark.implicits._
    val hfileCells = EmploymentDF.map(row => rowToEmployment(row)).flatMap(identity).rdd

    hfileCells.filter(_._2.value != null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(AssemblerConfiguration.PathToEmploymentHFile, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
    log.debug("End saveLeus")
  }
}
