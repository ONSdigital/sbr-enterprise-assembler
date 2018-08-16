package closures

import global.{AppParams, Configs}
import model.domain.HFileRow
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import spark.extensions.rdd.HBaseDataReader

/**
  *
  */
trait CalculateClosure extends AdminDataCalculator with BaseClosure with  RddLogging with Serializable{

  val confs = Configs.conf

  def updateCalculations(appconf:global.AppParams)(implicit spark:SparkSession) = {
    val allLUsDF: DataFrame = getExistingLousDF(appconf,confs)
    val allEntsDF: DataFrame = getExistingEntsDF(appconf,confs)
    val allLousDF: DataFrame = getExistingLousDF(appconf,confs)
    val calculatedDF = calculate(allLUsDF,appconf).cache()
    saveEntCalculations(calculatedDF,appconf)
    saveLouCalculations(calculatedDF,appconf)
  }

  def saveLouCalculations(calculationsDF:DataFrame,appconf:AppParams)(implicit spark:SparkSession) = {
    import spark.implicits._
    val hfileCells: RDD[(String, hfile.HFileCell)] = calculationsDF.map(row => rowToLouCalculations(row,appconf)).flatMap(identity(_)).rdd
    hfileCells.filter(_._2.value!=null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }


  def saveEntCalculations(calculationsDF:DataFrame,appconf:AppParams)(implicit spark:SparkSession) = {
    import spark.implicits._
    val hfileCells: RDD[(String, hfile.HFileCell)] = calculationsDF.map(row => rowToEntCalculations(row,appconf)).flatMap(identity(_)).rdd
    hfileCells.filter(_._2.value!=null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], Configs.conf)
  }

  def readLinksHFile(conf:global.AppParams)(implicit spark:SparkSession) = {
    val rows = HBaseDataReader.readEntitiesFromHFile[HFileRow](conf.PATH_TO_LINKS_HFILE)
    printRdd("rows",rows,"HFileRows")
  }
}

object CalculateClosure extends CalculateClosure
