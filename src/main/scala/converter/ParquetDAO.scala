package converter



import connector.HBaseConnector.getClass
import converter.ParquetDAO.logger
import global.Configured
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory



case class RowObject(key:String, colFamily:String, qualifier:String, value:String){
  def toKeyValue = new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, value.getBytes)
}

object ParquetDAO extends WithConversionHelper{

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String, parquetFilePath:String)(implicit spark:SparkSession):Unit = {
    val data: DataFrame = spark.read.json(jsonFilePath)
    data.write.parquet(parquetFilePath)
  }

  def parquetToHFile(parquetFilePath:String, pathToHFile:String)(implicit spark:SparkSession):Unit = {
    val parquetFileDF: DataFrame = spark.read.parquet(parquetFilePath)

    val data = parquetFileDF.rdd.flatMap(rowToEnt).sortBy(t => s"${t._2.key}${t._2.qualifier}")

    logger.debug("Start saving HFILE to HDFS...")
    data.map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue)).saveAsNewAPIHadoopFile(pathToHFile,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configured.conf)
    logger.debug("Finished saving HFILE to HDFS...")
    data.unpersist()
  }
}
