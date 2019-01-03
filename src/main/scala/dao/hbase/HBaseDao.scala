package dao.hbase

import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{PrefixFilter, RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import util.options.ConfigOptions

trait HBaseDao extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def readTable(config: Configuration, tableName: String)
               (implicit spark: SparkSession): RDD[HFileRow] = {
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    val res = readKvsFromHBase(config)
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def loadHFiles(implicit connection: Connection): Unit = {
    loadLinksHFile
    loadEnterprisesHFile
    loadLousHFile
  }

  def truncateTables()(implicit connection: Connection): Unit = {
    truncateLinksTable
    truncateEntsTable
    truncateLousTable
    truncateLeusTable
    truncateRusTable
  }

  def truncateTable(tableName: String)(implicit connection: Connection): Unit = wrapTransaction(tableName) { (table, admin) =>
    admin.disableTable(table.getName)
    admin.truncateTable(table.getName, true)
  }

  def truncateLinksTable(implicit connection: Connection): Unit = truncateTable(linksTableName)

  def truncateEntsTable(implicit connection: Connection): Unit = truncateTable(entsTableName)

  def truncateLousTable(implicit connection: Connection): Unit = truncateTable(lousTableName)

  def truncateLeusTable(implicit connection: Connection): Unit = truncateTable(leusTableName)

  def truncateRusTable(implicit connection: Connection): Unit = truncateTable(rusTableName)

  def readDeleteData(regex: String)(implicit spark: SparkSession): Unit = {
    val localConfCopy = ConfigOptions.hbaseConfiguration
    val data: RDD[HFileRow] = readLinksWithKeyFilter(localConfCopy, regex)
    val rows: Array[HFileRow] = data.take(5)
    rows.map(_.toString).foreach(row => print(
      "=" * 10 +
        row + '\n' +
        "=" * 10
    ))
  }

  def readLinksWithKeyFilter(confs: Configuration, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(confs, linksTableName, regex)
  }

  def readLinksWithKeyPrefixFilter(confs: Configuration, prefix: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithPrefixKeyFilter(confs, linksTableName, prefix)
  }

  def readLouWithKeyFilter(confs: Configuration, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(confs, lousTableName, regex)
  }

  def readEnterprisesWithKeyFilter(confs: Configuration, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {

    readTableWithKeyFilter(confs, entsTableName, regex)
  }

  def readTableWithPrefixKeyFilter(confs: Configuration, tableName: String, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    val localConfCopy = confs
    withKeyPrefixScanner(localConfCopy, regex, tableName) {
      readKvsFromHBase
    }
  }

  def readTableWithKeyFilter(confs: Configuration, tableName: String, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    val localConfCopy = confs
    withScanner(localConfCopy, regex, tableName) {
      readKvsFromHBase
    }
  }

  def loadRefreshLinksHFile(implicit connection: Connection): Unit = wrapTransaction(linksTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(ConfigOptions.PathToLinksHfile), admin, table, regionLocator)
  }

  def loadLinksHFile(implicit connection: Connection): Unit = wrapTransaction(linksTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(ConfigOptions.PathToLinksHfile), admin, table, regionLocator)
  }

  def loadEnterprisesHFile(implicit connection: Connection): Unit = wrapTransaction(entsTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(ConfigOptions.PathToEnterpriseHFile), admin, table, regionLocator)
  }

  def loadLousHFile(implicit connection: Connection): Unit = wrapTransaction(lousTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(ConfigOptions.PathToLocalUnitsHFile), admin, table, regionLocator)
  }

  def loadLeusHFile(implicit connection: Connection): Unit = wrapTransaction(leusTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(ConfigOptions.PathToLegalUnitsHFile), admin, table, regionLocator)
  }

  def loadRusHFile(implicit connection: Connection): Unit = wrapTransaction(rusTableName) { (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(ConfigOptions.PathToReportingUnitsHFile), admin, table, regionLocator)
  }

  private def wrapTransaction(fullTableName: String)(action: (Table, Admin) => Unit)(implicit connection: Connection) {
    val tn = TableName.valueOf(fullTableName)
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table, admin)
    table.close()
  }

  private def wrapReadTransaction(tableName: String)(action: String => RDD[HFileRow])(implicit connection: Connection): RDD[HFileRow] = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val admin = connection.getAdmin
    setJob(table)
    val res = action(tableName)
    table.close()
    res
  }

  private def setJob(table: Table)(implicit connection: Connection): Unit = {
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
    //HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(table.getName))
  }

  def withKeyPrefixScanner(config: Configuration, prefix: String, tableName: String)(getResult: Configuration => RDD[HFileRow]): RDD[HFileRow] = {
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    setPrefixScanner(config, prefix)
    val res = getResult(config)
    unsetPrefixScanner(config)
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def withScanner(config: Configuration, regex: String, tableName: String)(getResult: Configuration => RDD[HFileRow]): RDD[HFileRow] = {
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    setScanner(config, regex)
    val res = getResult(config)
    unsetScanner(config)
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def readKvsFromHBase(configuration: Configuration)(implicit spark: SparkSession): RDD[HFileRow] = {
    spark.sparkContext.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map(row => HFileRow(row._2))
  }

  def copyExistingRecordsToHFiles(dirName: String = "existing")(implicit spark: SparkSession): Unit = {
    def buildPath(path: String) = {
      val dirs = path.split("/")
      val updatedDirs = (dirs.init :+ dirName) :+ dirs.last
      val res = updatedDirs.mkString("/")
      res
    }

    val prevTimePeriod = (ConfigOptions.TimePeriod.toInt - 1).toString

    val ents: RDD[HFileRow] = HBaseDao.readEnterprisesWithKeyFilter(ConfigOptions.hbaseConfiguration, s"~$prevTimePeriod")
    val links: RDD[HFileRow] = HBaseDao.readLinksWithKeyFilter(ConfigOptions.hbaseConfiguration, s"~$prevTimePeriod")
    val lous: RDD[HFileRow] = HBaseDao.readLouWithKeyFilter(ConfigOptions.hbaseConfiguration, s".*~$prevTimePeriod~*.")

    ents.flatMap(_.toHFileCellRow(ConfigOptions.HBaseEnterpriseColumnFamily)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(buildPath(ConfigOptions.PathToEnterpriseHFile), classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], ConfigOptions.hbaseConfiguration)

    links.flatMap(_.toHFileCellRow(ConfigOptions.HBaseLinksColumnFamily)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(buildPath(ConfigOptions.PathToLinksHfile), classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], ConfigOptions.hbaseConfiguration)

    lous.flatMap(_.toHFileCellRow(ConfigOptions.HBaseLocalUnitsColumnFamily)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(buildPath(ConfigOptions.PathToLocalUnitsHFile), classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], ConfigOptions.hbaseConfiguration)
  }

  private def unsetScanner(config: Configuration): Unit = config.unset(TableInputFormat.SCAN)

  private def setScanner(config: Configuration, regex: String): Unit = {

    val comparator = new RegexStringComparator(regex)
    val filter = new RowFilter(CompareOp.EQUAL, comparator)

    def convertScanToString(scan: Scan): String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    val scan = new Scan()
    scan.setFilter(filter)
    val scanStr = convertScanToString(scan)

    config.set(TableInputFormat.SCAN, scanStr)
  }

  private def unsetPrefixScanner(config: Configuration): Unit = config.unset(TableInputFormat.SCAN)

  private def setPrefixScanner(config: Configuration, prefix: String): Unit = {

    val prefixFilter = new PrefixFilter(prefix.getBytes)
    val scan: Scan = new Scan()
    scan.setFilter(prefixFilter)

    def convertScanToString: String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }
    config.set(TableInputFormat.SCAN, convertScanToString)
  }

  def linksTableName = s"${ConfigOptions.HBaseLinksTableNamespace}:${ConfigOptions.HBaseLinksTableName}_${ConfigOptions.TimePeriod}"

  def leusTableName = s"${ConfigOptions.HBaseLegalUnitsNamespace}:${ConfigOptions.HBaseLegalUnitsTableName}_${ConfigOptions.TimePeriod}"

  def lousTableName = s"${ConfigOptions.HBaseLocalUnitsNamespace}:${ConfigOptions.HBaseLocalUnitsTableName}_${ConfigOptions.TimePeriod}"

  def rusTableName = s"${ConfigOptions.HBaseReportingUnitsNamespace}:${ConfigOptions.HBaseReportingUnitsTableName}_${ConfigOptions.TimePeriod}"

  def entsTableName = s"${ConfigOptions.HBaseEnterpriseTableNamespace}:${ConfigOptions.HBaseEnterpriseTableName}_${ConfigOptions.TimePeriod}"

}

object HBaseDao extends HBaseDao