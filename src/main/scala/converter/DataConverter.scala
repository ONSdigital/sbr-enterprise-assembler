package converter



import connector.HBaseConnector
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession,Row}

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
        case _ => throw new Exception(s"cannot do bytes from this string: $s")
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

      val period = "201802"
      val idKey = "id"

      val data: RDD[(ImmutableBytesWritable, KeyValue)] = parquetFileDF.rdd.sortBy(_.getAs[Long]("id")).map(r => {
        val id: Long = r.getAs[Long](idKey)
        val keyStr = s"$period~${id}~ENT"
        val row = new KeyValue(strToBytes(keyStr), strToBytes(config.getString("hbase.table.column.family")), longToBytes(id), strToBytes("ENT") )
        val key =  strToBytes(keyStr)
        (new ImmutableBytesWritable(key), row)
      })

      def rowToEnt(r:Row): (ImmutableBytesWritable, KeyValue) = {
        val ern = java.util.UUID.randomUUID().toString
        val ubnr: Long = r.getAs[Long](idKey)
        val keyStr = s"$period~${ern}~ENT"
        val row = new KeyValue(strToBytes(keyStr), strToBytes(config.getString("hbase.table.column.family")), longToBytes(ubnr), strToBytes("legalunit") )
        val key =  strToBytes(keyStr)
        (new ImmutableBytesWritable(key), row)
      }

/*  def getVats(vats:Seq[String], ubrn:Long) = {
    vats.map(vat => r.)
  }*/

     def rowToLegalUnit(r:Row, ern:String):(ImmutableBytesWritable, KeyValue) = {
       val ubnr: Long = r.getAs[Long](idKey)
       val keyStr = s"$period~${ubnr}~LEU"
       val row = new KeyValue(strToBytes(keyStr), strToBytes(config.getString("hbase.table.column.family")), strToBytes(ern), strToBytes("enterprise") )
       val key =  strToBytes(keyStr)
       (new ImmutableBytesWritable(key), row)
     }

      import HBaseConnector._

      data.saveAsNewAPIHadoopFile(pathToHFile,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],conf)

      }




}
