package spark.extensions.rdd

import global.Configs.conf
import model.domain.{HFileRow, KVCell}
import org.apache.crunch.io.hbase.HFileInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
/**
  *
  */
trait HBaseDataReader{

        type DataMap = (String,Iterable[(String, String)])

        def getKeyValue[T <: Cell](kv:T): (String, (String, String)) =
              (Bytes.toString(kv.getRowArray).slice(kv.getRowOffset, kv.getRowOffset + kv.getRowLength),

              (Bytes.toString(kv.getQualifierArray).slice(kv.getQualifierOffset,
                      kv.getQualifierOffset + kv.getQualifierLength),
                      Bytes.toString(kv.getValueArray).slice(kv.getValueOffset,
                      kv.getValueOffset + kv.getValueLength)))





        def readEntitiesFromHFile[T:ClassTag](hfilePath:String)(implicit spark:SparkSession, readEntity:DataMap => T ): RDD[T] = {
          val confLocalCopy = conf
          spark.sparkContext.newAPIHadoopFile(
            hfilePath,
            classOf[HFileInputFormat],
            classOf[NullWritable],
            classOf[KeyValue],
            confLocalCopy
          ).map(v => getKeyValue(v._2)).groupByKey().map(entity => readEntity(entity))
        }



//RDD[(String, hfile.HFileCell)]
        def readKvsFromHFile(hfilePath:String)(implicit spark:SparkSession): RDD[DataMap] = {
          val confLocalCopy = conf
          spark.sparkContext.newAPIHadoopFile(
            hfilePath,
            classOf[HFileInputFormat],
            classOf[NullWritable],
            classOf[KeyValue],
            confLocalCopy
          ).map(v => getKeyValue(v._2)).groupByKey()

        }

        def readKvsFromHBase(configuration:Configuration)(implicit spark:SparkSession): RDD[HFileRow] =  {
          spark.sparkContext.newAPIHadoopRDD(
            configuration,
            classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])
            .map(_._2).map(HFileRow(_))
        }







}
