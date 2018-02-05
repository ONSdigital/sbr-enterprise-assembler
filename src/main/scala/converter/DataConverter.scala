package converter

import connector.HBaseConnector
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  */
object DataConverter {

import global.ApplicationContext._

  def jsonToParquet(jsonFilePath:String, parquetFilePath:String)(implicit spark:SparkSession):Unit = {
    val data: DataFrame = spark.read.json(jsonFilePath)
    data.write.parquet(parquetFilePath)
  }

def parquetToHFile()(implicit spark:SparkSession):Unit = parquetToHFile(config.getString("files.parquet"))

def parquetToHFile(parquetFilePath:String, pathToHFile:String = config.getString("files.hfile"))(implicit spark:SparkSession):Unit = {

      def strToBytes(s:String) = try{
        s.getBytes()
      }catch{
        case e:Throwable => {
          throw new Exception(e)
        }
        case _ => throw new Exception(s"cannot do bytes from string: $s")
      }
      def longToBytes(l:Long) = try{
        Bytes.toBytes(l)
      }catch{
        case e:Throwable => {
          throw new Exception(e)
        }
        case _ => throw new Exception(s"cannot do bytes from long: $l")
      }

      val parquetFileDF: DataFrame = spark.read.parquet(parquetFilePath)

        /*//TEST:
            parquetFileDF.createOrReplaceTempView("businessIndexRec")
            val namesDF: DataFrame = spark.sql("SELECT VatRefs,PayeRefs FROM businessIndexRec WHERE BusinessName = 'NICHOLAS ROSS PLC'")
            namesDF.show()*/

      val period = "201706"
      val tableName = "enterprise"
      val colFamily = "d"
      val idKey = "id"

      val data: RDD[(ImmutableBytesWritable, KeyValue)] = parquetFileDF.rdd.sortBy(_.getAs[Long]("id")).map(r => {
        val id: Long = r.getAs[Long](idKey)
        val keyStr = s"${id}~$period~ENT"
        val row = new KeyValue(strToBytes(keyStr), strToBytes(colFamily), longToBytes(id), strToBytes("ENT") )
        val key =  strToBytes(keyStr)
        (new ImmutableBytesWritable(key), row)
      })

      import HBaseConnector._

      data.saveAsNewAPIHadoopFile(pathToHFile,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],conf)

      }




}
