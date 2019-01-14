package dao.hbase

import model.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import util.configuration.AssemblerConfiguration
import util.configuration.AssemblerConfiguration._

object HBaseDao extends Serializable {

  def readTable(config: Configuration, tableName: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    val res = readKvsFromHBase(config)
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def truncateTables()(implicit connection: Connection): Unit = {
    truncateTable(linksTableName)
    truncateTable(entsTableName)
    truncateTable(lousTableName)
    truncateTable(leusTableName)
    truncateTable(rusTableName)
  }

  def truncateTable(tableName: String)(implicit connection: Connection): Unit = wrapTransaction(tableName) { (table, admin) =>
    admin.disableTable(table.getName)
    admin.truncateTable(table.getName, true)
  }

  def loadLinksHFile(implicit connection: Connection): Unit = wrapTransaction(linksTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PathToLinksHfile), admin, table, regionLocator)
  }

  def loadEnterprisesHFile(implicit connection: Connection): Unit = wrapTransaction(entsTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PathToEnterpriseHFile), admin, table, regionLocator)
  }

  def loadLousHFile(implicit connection: Connection): Unit = wrapTransaction(lousTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PathToLocalUnitsHFile), admin, table, regionLocator)
  }

  def loadLeusHFile(implicit connection: Connection): Unit = wrapTransaction(leusTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PathToLegalUnitsHFile), admin, table, regionLocator)
  }

  def loadRusHFile(implicit connection: Connection): Unit = wrapTransaction(rusTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PathToReportingUnitsHFile), admin, table, regionLocator)
  }

  private def wrapTransaction(fullTableName: String)(action: (Table, Admin) => Unit)(implicit connection: Connection) {
    val tn = TableName.valueOf(fullTableName)
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table, admin)
    table.close()
  }

  private def setJob(table: Table)(implicit connection: Connection): Unit = {
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }

  private def readKvsFromHBase(configuration: Configuration)(implicit spark: SparkSession): RDD[HFileRow] = {
    spark.sparkContext.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map(row => HFileRow(row._2))
  }

  def linksTableName = s"${AssemblerConfiguration.HBaseLinksTableNamespace}:${AssemblerConfiguration.HBaseLinksTableName}_${AssemblerConfiguration.TimePeriod}"

  def leusTableName = s"${AssemblerConfiguration.HBaseLegalUnitsNamespace}:${AssemblerConfiguration.HBaseLegalUnitsTableName}_${AssemblerConfiguration.TimePeriod}"

  def lousTableName = s"${AssemblerConfiguration.HBaseLocalUnitsNamespace}:${AssemblerConfiguration.HBaseLocalUnitsTableName}_${AssemblerConfiguration.TimePeriod}"

  def rusTableName = s"${AssemblerConfiguration.HBaseReportingUnitsNamespace}:${AssemblerConfiguration.HBaseReportingUnitsTableName}_${AssemblerConfiguration.TimePeriod}"

  def entsTableName = s"${AssemblerConfiguration.HBaseEnterpriseTableNamespace}:${AssemblerConfiguration.HBaseEnterpriseTableName}_${AssemblerConfiguration.TimePeriod}"

}