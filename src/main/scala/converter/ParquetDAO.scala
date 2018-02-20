package converter



import global.Configured
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory



case class RowObject(key:String, colFamily:String, qualifier:String, value:String){
  def toKeyValue = new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, value.getBytes)
}

case class Tables(enterprises: Seq[(String, RowObject)],links:Seq[(String, RowObject)])

object ParquetDAO extends WithConversionHelper{

  import Configured._

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String)(implicit spark:SparkSession):Unit = {
    val data: DataFrame = spark.read.json(jsonFilePath)
    data.write.parquet(PATH_TO_PARQUET)
  }

  def parquetToHFile(implicit spark:SparkSession):Unit = {

    val parquetFileDF: DataFrame = spark.read.parquet(PATH_TO_PARQUET)

    val parquetRDD = parquetFileDF.rdd.cache().map(toRecords).cache()

    parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configured.conf)

    parquetRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configured.conf)

    parquetRDD.unpersist()

  }
}
