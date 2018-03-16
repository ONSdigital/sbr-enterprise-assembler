package spark.extensions.rdd

import global.Configs.conf
import org.apache.crunch.io.hbase.HFileInputFormat
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
/**
  *
  */
object HBaseDataReader{

        type DataMap = (String,Iterable[(String, String)])

        def getKeyValue(kv:KeyValue): (String, (String, String)) =
              (Bytes.toString(kv.getRowArray).slice(kv.getRowOffset, kv.getRowOffset + kv.getRowLength),

              (Bytes.toString(kv.getQualifierArray).slice(kv.getQualifierOffset,
                      kv.getQualifierOffset + kv.getQualifierLength),
                      Bytes.toString(kv.getValueArray).slice(kv.getValueOffset,
                      kv.getValueOffset + kv.getValueLength)))



        def readEntitiesFromHFile[T:ClassTag](hfilePath:String)(implicit spark:SparkSession, readEntity:DataMap => T ): RDD[T] =
                                              spark.sparkContext.newAPIHadoopFile(
                                                hfilePath,
                                                classOf[HFileInputFormat],
                                                classOf[NullWritable],
                                                classOf[KeyValue],
                                                conf
                                              ).map(v => getKeyValue(v._2)).groupByKey().map(entity => readEntity(entity))



        def readKvsFromHFile(hfilePath:String)(implicit spark:SparkSession): RDD[(String,Iterable[(String, String)])] =
                                              spark.sparkContext.newAPIHadoopFile(
                                                hfilePath,
                                                classOf[HFileInputFormat],
                                                classOf[NullWritable],
                                                classOf[KeyValue],
                                                conf
                                              ).map(v => getKeyValue(v._2)).groupByKey()






}
