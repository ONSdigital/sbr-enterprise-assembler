package dao.parquet

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import global.{AppParams, Configs}
import model.domain.HFileRow
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
import spark.calculations.DataFrameHelper
import spark.extensions.sql.SqlRowExtensions

object ParquetDAO extends WithConversionHelper with DataFrameHelper{

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String)(implicit spark:SparkSession,appconf:AppParams) = spark.read.json(jsonFilePath).write.parquet(appconf.PATH_TO_PARQUET)

  def parquetToHFile(implicit spark:SparkSession,appconf:AppParams){

    val appArgs = appconf

    val parquetRDD: RDD[hfile.Tables] = finalCalculations(spark.read.parquet(appconf.PATH_TO_PARQUET), spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.map(row => toNewEnterpriseRecords(row,appArgs)).cache()

        parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
          .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
              .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

        parquetRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
          .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
              .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

        parquetRDD.unpersist()
  }

    def readParquet(appconf:AppParams)(implicit spark:SparkSession)  = {
      val parquetRDD: RDD[(String, hfile.HFileCell)] = spark.read.parquet(appconf.PATH_TO_PARQUET).rdd.flatMap(row => toLinksRefreshRecords(row,appconf))
      parquetRDD
    }


    def readParquetIntoHFileRow(appconf:AppParams)(implicit spark:SparkSession)  = {
      val parquetRDD: RDD[(String, hfile.HFileCell)] = spark.read.parquet(appconf.PATH_TO_PARQUET).rdd.flatMap(row => toLinksRefreshRecords(row,appconf))
      parquetRDD
    }



    def createRefreshLinksHFile(appconf:AppParams)(implicit spark:SparkSession) = {


      val parquetRDD: RDD[(String, hfile.HFileCell)] = spark.read.parquet(appconf.PATH_TO_PARQUET).rdd.flatMap(row => toLinksRefreshRecords(row,appconf))

      parquetRDD.sortBy(t => s"${t._2.key}${t._2.qualifier}")
        .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
        .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE_UPDATE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
      }



    def createEnterpriseRefreshHFile(appconf:AppParams)(implicit spark:SparkSession,connection:Connection) = {
            val localConfigs = Configs.conf
            val regex = "~LEU~"+{appconf.TIME_PERIOD}+"$"
            val lus: RDD[HFileRow] = HBaseDao.readLinksWithKeyFilter(localConfigs,appconf,regex) //read LUs from links

            val rows: RDD[Row] = lus.map(row => Row(row.getId, row.cells.find(_.column == "p_ENT").get.value)) //extract ERNs

            val schema = new StructType()
              .add(StructField("id", StringType, true))
              .add(StructField("ern", StringType, true))

            val erns = spark.createDataFrame(rows,schema)

            val refreshDF = spark.read.parquet(appconf.PATH_TO_PARQUET)

            val fullLUs = refreshDF.join(erns,"id")

            //get cells for jobs and employees - the only updateable columns in enterprise table
            val entsRDD: RDD[(String, hfile.HFileCell)] = finalCalculations(fullLUs, spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.flatMap(row => Seq(
              ParquetDAO.createEnterpriseCell(row.getString("ern").get,"paye_employees",row.getCalcValue("paye_employees").get,appconf),
              ParquetDAO.createEnterpriseCell(row.getString("ern").get,"paye_jobs",row.getCalcValue("paye_jobs").get,appconf)
            ))

            entsRDD.sortBy(t => s"${t._2.key}${t._2.qualifier}").map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
              .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    }


}