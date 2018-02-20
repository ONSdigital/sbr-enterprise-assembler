package converter



import global.Configs
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory



case class RowObject(key:String, colFamily:String, qualifier:String, value:String){
  val logger = LoggerFactory.getLogger(getClass)
  def toKeyValue = try{new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, value.getBytes)} catch {

    case npe:NullPointerException => {
      logger.error(s"NullPointerException for RowObject: ${this.toString}")
      throw npe
    }
    case e:Throwable => {
      logger.error(s"Exception for RowObject: ${this.toString}")
      throw e
    }
  }
}

case class Tables(enterprises: Seq[(String, RowObject)],links:Seq[(String, RowObject)])

object ParquetDAO extends WithConversionHelper{

  import Configs._

  val logger = LoggerFactory.getLogger(getClass)

  def jsonToParquet(jsonFilePath:String)(implicit spark:SparkSession){
    val data: DataFrame = spark.read.json(jsonFilePath)
    data.write.parquet(PATH_TO_PARQUET)
  }

  def parquetToHFile(implicit spark:SparkSession){

    val parquetFileDF: DataFrame = spark.read.parquet(PATH_TO_PARQUET)

    val parquetRDD = parquetFileDF.rdd.cache().map(toRecords).cache()

    parquetRDD.flatMap(_.links).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.flatMap(_.enterprises).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
          .saveAsNewAPIHadoopFile(PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    parquetRDD.unpersist()

  }
}
