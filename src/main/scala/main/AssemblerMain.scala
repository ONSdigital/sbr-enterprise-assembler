package main


import connector.HBaseConnector
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  *
  */
object AssemblerMain extends App{

  override def main(args: Array[String]) {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("enterprise assembler")
      .config("spark.ui.port", "18080")
      .getOrCreate()

    //JsonToParquet.convertAndSave(spark)

    val parquetFileDF: DataFrame = spark.read.parquet("src/main/resources/sample.parquet")

    //parquetFileDF.printSchema()

    val path = "src/main/resources/hfile"
    val period = "201706"
    val tableName = "enterprise"
    val colFamily = "d"
    val idKey = "id"
    val hfilePath = "src/main/resources/hfile"

  /*parquetFileDF.createOrReplaceTempView("businessIndexRec")
    val namesDF: DataFrame = spark.sql("SELECT VatRefs,PayeRefs FROM businessIndexRec WHERE BusinessName = 'NICHOLAS ROSS PLC'")
    namesDF.show()
  */
    def strToBytes(s:String) = try{
      s.getBytes()
    }catch{
      case e:Exception => {
        throw new Exception(e)
      }
      case _ => throw new Exception(s"cannot do bytes from string: $s")
    }
    def longToBytes(l:Long) = try{
      Bytes.toBytes(l)
    }catch{
      case e:Exception => {
        throw new Exception(e)
      }
      case _ => throw new Exception(s"cannot do bytes from long: $l")
    }

    val data: RDD[(ImmutableBytesWritable, KeyValue)] = parquetFileDF.rdd.sortBy(_.getAs[Long]("id")).map(r => {
      val id: Long = r.getAs[Long](idKey)
      val keyStr = s"${id}~$period~ENT"
      //println(s"XXXXXXXXXXXXX ID=$id")
      val row = new KeyValue(strToBytes(keyStr), strToBytes(colFamily), longToBytes(id), strToBytes("ENT") )
      val key =  strToBytes(keyStr)
      (new ImmutableBytesWritable(key), row)
    })

    //data.saveAsNewAPIHadoopFile(hfilePath,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],config)
    //HBaseConnector.loadHFile(hfilePath)
    spark.stop()

  }
}
