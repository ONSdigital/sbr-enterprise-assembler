package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import global.{AppParams, Configs}
import model.domain.{HFileRow, KVCell}
import model.hfile
import model.hfile.{HFileCell, Tables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.RddLogging
import spark.calculations.{AdminDataCalculator, DataFrameHelper}
import spark.extensions.sql._

import scala.util.Try

trait NewPeriodWithCalculations extends WithConversionHelper with DataFrameHelper with AdminDataCalculator with RddLogging with Serializable{


}
object NewPeriodWithCalculations extends NewPeriodWithCalculations