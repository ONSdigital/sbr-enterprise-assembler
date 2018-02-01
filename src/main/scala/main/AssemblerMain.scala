package main


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
    val id = "ID"

  /*parquetFileDF.createOrReplaceTempView("businessIndexRec")
    val namesDF: DataFrame = spark.sql("SELECT VatRefs,PayeRefs FROM businessIndexRec WHERE BusinessName = 'NICHOLAS ROSS PLC'")
    namesDF.show()
  */
    def strB(s:String) = try{
      s.getBytes()
    }catch{
      case e:Exception => {
        throw new Exception(e)
      }
      case _ => throw new Exception(s"cannot do bytes from string: $s")
    }
    def longB(l:Long) = try{
      Bytes.toBytes(l)
    }catch{
      case e:Exception => {
        throw new Exception(e)
      }
      case _ => throw new Exception(s"cannot do bytes from long: $l")
    }

    val data: RDD[(ImmutableBytesWritable, KeyValue)] = parquetFileDF.rdd.sortBy(_.getAs[Long]("id")).map(r => {
      val id: Long = r.getAs[Long]("id")
      val keyStr = s"${id}~$period~ENT"
      //println(s"XXXXXXXXXXXXX ID=$id")
      val row = new KeyValue(strB(keyStr), strB(colFamily), longB(id), strB("ENT") )
      val key =  strB(keyStr)
      (new ImmutableBytesWritable(key), row)
    })

    val config = HBaseConfiguration.create()
    config.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    data.saveAsNewAPIHadoopFile("src/main/resources/hfile",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],config)
  }
}
