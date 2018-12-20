package spark.extensions.rdd

import global.Configs.conf
import org.apache.crunch.io.hbase.HFileInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, KeyValue}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
/**
  *
  */
object HBaseDataReader{

        type DataMap = (String,Iterable[(String, String)])

        def getKeyValue[T <: Cell](kv:T): (String, (String, String)) = {
          val key = Bytes.toString(CellUtil.cloneRow(kv))
          val column =  Bytes.toStringBinary(CellUtil.cloneQualifier(kv))
          val value = Bytes.toStringBinary(CellUtil.cloneValue(kv))
          (key,(column, value))

        }


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

}


