package closures

import dao.hbase.{HBaseDao, HFileUtils}
import dao.hbase.converter.WithConversionHelper
import dao.parquet.ParquetDao
import dao.parquet.ParquetDao.adminCalculations
import global.Configs.conf
import global.{AppParams, Configs}
import model.domain.HFileRow
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spark.calculations.{AdminDataCalculator, DataFrameHelper}
import spark.extensions.sql.SqlRowExtensions
/**
  *
  */
trait RefreshClosures extends HFileUtils with AdminDataCalculator with Serializable{



  def readDeleteData(appconf:AppParams)(implicit ss:SparkSession,connection:Connection){//.*(?!~ENT~)201802$
    val regex = ".*(?<!ENT)~"+{appconf.TIME_PERIOD}+"$"
    HBaseDao.readDeleteData(appconf,regex)
  }


  def createDeleteLinksHFile(appconf:AppParams)(implicit ss:SparkSession,connection:Connection){
    val regex = ".*(?<!(ENT|LOU))~"+{appconf.TIME_PERIOD}+"$"
    HBaseDao.saveDeleteLinksToHFile(appconf,regex)
  }

  def createLinksRefreshHFile(appconf:AppParams)(implicit spark:SparkSession) = ParquetDao.createRefreshLinksHFile(appconf)

  def createEnterpriseRefreshHFile(appconf:AppParams)(implicit spark:SparkSession,connection:Connection) = {
    val localConfCopy = conf
    val regex = "~LEU~"+{appconf.TIME_PERIOD}+"$"
    val lus: RDD[HFileRow] = HBaseDao.readLinksWithKeyFilter(localConfCopy,appconf,regex) //read LUs from links

    val rows: RDD[Row] = lus.map(row => Row(row.getId, row.cells.find(_.column == "p_ENT").get.value)) //extract ERNs

    val schema = new StructType()
      .add(StructField("id", StringType, true))
      .add(StructField("ern", StringType, true))

    val erns = spark.createDataFrame(rows,schema)

    val refreshDF = spark.read.parquet(appconf.PATH_TO_PARQUET)
  /**
   * DF BI data with ern for each LU
   * */
    val fullLUs: DataFrame = refreshDF.join(erns,"id")

    val payeDF = spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)
    val vatDF  = spark.read.option("header", "true").csv(appconf.PATH_TO_VAT)

    //get cells for jobs and employees - the only updateable columns in enterprise table
    val entsRDD: RDD[(String, hfile.HFileCell)] = calculate(fullLUs,appconf).rdd.flatMap(row => {
      val ern = row.getString("ern").get
      Seq(
      row.getCalcValue("paye_employees").map(employees => createEnterpriseCell(ern, "payeemployees", employees, appconf)),
      row.getCalcValue("paye_jobs").map(jobs => createEnterpriseCell(ern, "payejobs", jobs, appconf)),
      row.getCalcValue("apportion_turnover").map(apportion => createEnterpriseCell(ern, "app_turnover", apportion, appconf)),
      row.getCalcValue("total_turnover").map(total => createEnterpriseCell(ern, "ent_turnover", total, appconf)),
      row.getCalcValue("temp_contained_rep_vat_turnover").map(contained => createEnterpriseCell(ern, "cntd_turnover", contained, appconf)),
      row.getCalcValue("temp_standard_vat_turnover").map(standard => createEnterpriseCell(ern, "std_turnover", standard, appconf)),
      row.getCalcValue("group_turnover").map(group => createEnterpriseCell(ern, "grp_turnover", group, appconf))
    ).collect { case Some(v) => v }})

    entsRDD.sortBy(t => s"${t._2.key}${t._2.qualifier}").map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

  }


  def loadRefreshFromHFiles(appconf:AppParams)(implicit con: Connection) = {

    HBaseDao.loadDeleteLinksHFile(con,appconf)
    HBaseDao.loadRefreshLinksHFile(con,appconf)
    HBaseDao.loadEnterprisesHFile(con,appconf)

  }
}

object RefreshClosures extends RefreshClosures
