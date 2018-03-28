package executors

import dao.hbase.HBaseDao
import dao.parquet.ParquetDAO
import dao.parquet.ParquetDAO.finalCalculations
import global.Configs.conf
import global.{AppParams, Configs}
import model.domain.HFileRow
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spark.extensions.sql.SqlRowExtensions
/**
  *
  */
object RefreshClosures {

  def createDeleteLinksHFile(appconf:AppParams)(implicit ss:SparkSession){
    val regex = ".*(?<!~ENT~"+{appconf.TIME_PERIOD}+")$"
    HBaseDao.saveDeleteLinksToHFile(appconf,regex)
  }

  def createLinksRefreshHFile(appconf:AppParams)(implicit spark:SparkSession) = ParquetDAO.createRefreshLinksHFile(appconf)

  def createEnterpriseRefreshHFile(appconf:AppParams)(implicit spark:SparkSession) = {

    val regex = "~LEU~"+{appconf.TIME_PERIOD}+"$"
    val lus: RDD[HFileRow] = HBaseDao.readWithKeyFilter(appconf,regex) //read LUs from links

    val rows: RDD[Row] = lus.map(row => Row(row.getId, row.cells.find(_.column == "p_ENT").get.value)) //extract ERNs

    val schema = new StructType()
      .add(StructField("id", StringType, true))
      .add(StructField("ern", StringType, true))

    val erns = spark.createDataFrame(rows,schema)

    val refreshDF = spark.read.parquet(appconf.PATH_TO_PARQUET)

    val fullLUs = refreshDF.join(erns,"id")

    //get cells for jobs and employees - the only updateable columns in enterprise table
    val entsRDD: RDD[(String, hfile.HFileCell)] = finalCalculations(fullLUs, spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)).rdd.flatMap(row => Seq(
      ParquetDAO.createEnterpriseCell(row.getString("ern").get,"paye_employees",row.getInt("paye_employees").map(_.toString).getOrElse("0"),appconf),
      ParquetDAO.createEnterpriseCell(row.getString("ern").get,"paye_jobs",row.getInt("paye_jobs").map(_.toString).getOrElse("0"),appconf)
    ))

    entsRDD.sortBy(t => s"${t._2.key}${t._2.qualifier}").map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

  }


  def loadRefreshFromHFiles(appconf:AppParams)(implicit con: Connection) = {

    HBaseDao.loadDeleteLinksHFile(con,appconf)
    HBaseDao.loadRefreshLinksHFile(con,appconf)
    HBaseDao.loadEnterprisesHFile(con,appconf)

  }
}
