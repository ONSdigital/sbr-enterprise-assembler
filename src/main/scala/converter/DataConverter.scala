package converter



import connector.HBaseConnector
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

/**
  *
  */
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

      parquetFileDF.printSchema()
        /*//TEST:
            parquetFileDF.createOrReplaceTempView("businessIndexRec")
            val namesDF: DataFrame = spark.sql("SELECT VatRefs,PayeRefs FROM businessIndexRec WHERE BusinessName = 'NICHOLAS ROSS PLC'")
            namesDF.show()*/

  val parquetRdd = parquetFileDF.rdd//.sortBy(_.getAs[Long]("id"))
  parquetFileDF.printSchema()
/*  val data: RDD[(ImmutableBytesWritable, KeyValue)] = parquetRdd.flatMap(rowToEnt).sortBy(_._1).map(kv => {
    val key = new ImmutableBytesWritable(toBytes(kv._1))
    println(s"KEY: ${kv._1}")
    (key,kv._2)
  })  */

  val flatt: RDD[(String, RowObject)] = parquetFileDF.rdd.flatMap(rowToEnt)
  val sorted = flatt.sortBy(_._1)

  //val data: RDD[(ImmutableBytesWritable, KeyValue)] =  sorted.map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))//parquetRdd.sortBy(_.getAs[Long]("id")).map(rowToEnt)

/*val collected: Array[(ImmutableBytesWritable, KeyValue)] = data.collect()
  println(collected.toString)*/

      import HBaseConnector._

  sorted.map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue)).saveAsNewAPIHadoopFile(pathToHFile,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],conf)
  sorted.unpersist()
      }


}
