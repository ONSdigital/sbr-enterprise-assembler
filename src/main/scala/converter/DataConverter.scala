package converter



import connector.HBaseConnector
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.sql.{DataFrame, SparkSession}



case class RowObject(key:String, colFamily:String, qualifier:String, value:String){
  def toKeyValue = new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, value.getBytes)
}


object DataConverter extends WithConversionHelper{

import global.ApplicationContext._

  def jsonToParquet(jsonFilePath:String, parquetFilePath:String)(implicit spark:SparkSession):Unit = {
    val data: DataFrame = spark.read.json(jsonFilePath)
    data.write.parquet(parquetFilePath)
  }

  def jsonToParquet(implicit spark:SparkSession):Unit = {
    val data: DataFrame = spark.read.json(PATH_TO_JSON)
    data.write.parquet(PATH_TO_PARQUET)
  }



def parquetToHFile()(implicit spark:SparkSession):Unit = parquetToHFile(PATH_TO_PARQUET)

def parquetToHFile(parquetFilePath:String, pathToHFile:String = PATH_TO_HFILE)(implicit spark:SparkSession):Unit = {



      val parquetFileDF: DataFrame = spark.read.parquet(parquetFilePath)

      val data = parquetFileDF.rdd.flatMap(rowToEnt).sortBy(t => s"${t._2.key}${t._2.qualifier}")

     import global.ApplicationContext._

      data.map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue)).saveAsNewAPIHadoopFile(pathToHFile,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],conf)
      data.unpersist()
  }


}
