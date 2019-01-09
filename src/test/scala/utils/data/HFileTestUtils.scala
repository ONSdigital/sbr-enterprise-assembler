package utils.data

import dao.hbase.HFilePartitioner
import model.{HFileCell, HFileRow}
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import util.configuration.AssemblerConfiguration
import util.configuration.AssemblerHBaseConfiguration._

trait HFileTestUtils {

  def sortByKeyAndEntityName(row: HFileRow): String = {

    val keyBlocks = row.key.split("~")
    val id = keyBlocks.last
    val entityName = keyBlocks.head
    entityName + id
  }

  def saveLinksToHFile(rows: Seq[HFileRow], colFamily: String, path: String)(implicit spark: SparkSession, connection: Connection): Unit = {
    val records: RDD[HFileRow] = spark.sparkContext.parallelize(rows).map(row => row.copy(cells = row.cells.toList.sortBy(_.column)))
    val cells: RDD[(String, HFileCell)] = records.flatMap(_.toHFileCellRow(colFamily))

    val tableName = TableName.valueOf(s"${AssemblerConfiguration.HBaseLinksTableNamespace}:${AssemblerConfiguration.HBaseLinksTableName}_${AssemblerConfiguration.TimePeriod}")
    val regionLocator = connection.getRegionLocator(tableName)
    val partitioner = HFilePartitioner(connection.getConfiguration, regionLocator.getStartKeys, 1)

    val repartitionedCells: RDD[((String, String), HFileCell)] = cells.map(entry => ((entry._1, entry._2.qualifier), entry._2)).repartitionAndSortWithinPartitions(partitioner)
    repartitionedCells.map(rec => (new ImmutableBytesWritable(rec._1._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
  }

  def saveToHFile(rows: Seq[HFileRow], colFamily: String, path: String)(implicit spark: SparkSession): Unit = {
    val records: RDD[HFileRow] = spark.sparkContext.parallelize(rows)
    val cells: RDD[(String, HFileCell)] = records.flatMap(_.toHFileCellRow(colFamily))
    cells.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfiguration)
  }

}
