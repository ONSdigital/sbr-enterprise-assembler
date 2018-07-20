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

trait NewCreateClosure extends WithConversionHelper with UnitRowConverter with DataFrameHelper with AdminDataCalculator with RddLogging with Serializable{

  val hbaseDao: HBaseDao = HBaseDao

  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession): Unit = {
    val confs = Configs.conf

    val updatedConfs = appconf.copy(TIME_PERIOD = appconf.PREVIOUS_TIME_PERIOD)
    val parquetDF = spark.read.parquet(appconf.PATH_TO_PARQUET)
    //printDF("update parquet Schema:",parquetDF)


    //val parquetRows: RDD[Row] = parquetDF.rdd
    // printRddOfRows("parquetRows",parquetRows)

    val incomingBiData: DataFrame = parquetDF.castAllToString
  }

  def getExistingLous(appconf:AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val linksTableName = s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
    val luRegex = ".*(ENT|LEU)~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val existingLinks: RDD[HFileRow] = hbaseDao.readTableWithKeyFilter(confs,appconf, linksTableName, luRegex)
    import spark.sqlContext.implicits._
    val leLinks = existingLinks.collect{case row@HFileRow(key,cells) if(key.contains("~LEU~"))=> row.toLuRow}.toDF()
    val entLInks = existingLinks.collect{case row@HFileRow(key,cells) if(key.contains("~ENT~"))=> row.toEntRow}

  }

}
object NewCreateClosure extends NewCreateClosure
