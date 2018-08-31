package utils

import global.{AppParams, Configs}
import model.domain._
import model.hfile
import model.hfile.HFileCell
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/**
  *
  */
trait HFileTestUtils {




  def entToHFileCells(ents:RDD[HFileRow])(implicit configs: AppParams) = ents.flatMap(row =>
              row.cells.map(cell => HFileCell(row.key,configs.HBASE_ENTERPRISE_COLUMN_FAMILY,cell.column,cell.value)))


  def linksToHFileCells(ents:RDD[HFileRow])(implicit configs: AppParams) = ents.flatMap(row =>
              row.cells.map(cell => HFileCell(row.key,configs.HBASE_LINKS_COLUMN_FAMILY,cell.column,cell.value)))


  def localUnitsToHFileCells(ents:RDD[HFileRow])(implicit configs: AppParams) = ents.flatMap(row =>
              row.cells.map(cell => HFileCell(row.key,configs.HBASE_LOCALUNITS_COLUMN_FAMILY,cell.column,cell.value)))


  def sortByKeyAndEntityName(row:HFileRow) = {

    val keyBlocks = row.key.split("~")
    val id = keyBlocks.last
    val entityName = keyBlocks.head
    entityName+id
  }

  def saveLinksToHFile(rows:Seq[HFileRow], colFamily:String, appconf:AppParams, path:String)(implicit spark:SparkSession) = {
    val records: RDD[HFileRow] = spark.sparkContext.parallelize(rows).map(row => row.copy(cells = row.cells.toList.sortBy(_.column)))
    val cells: RDD[(String, hfile.HFileCell)] = records.flatMap(_.toHFileCellRow(colFamily))
    cells.sortBy(cell => cell._1)
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)
  }

  def saveToHFile(rows:Seq[HFileRow], colFamily:String, appconf:AppParams, path:String)(implicit spark:SparkSession) = {
    val records: RDD[HFileRow] = spark.sparkContext.parallelize(rows)
    val cells: RDD[(String, hfile.HFileCell)] = records.flatMap(_.toHFileCellRow(colFamily))
    cells.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)
  }

}
