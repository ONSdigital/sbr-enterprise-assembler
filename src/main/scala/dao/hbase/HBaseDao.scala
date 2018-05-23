package dao.hbase

import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  *
  */
object HBaseDao{
  import global.Configs._

  val logger = LoggerFactory.getLogger(getClass)



  def loadHFiles(implicit connection:Connection,appParams:AppParams) = {
    loadLinksHFile
    loadEnterprisesHFile
    loadLousDeleteHFile
  }

  def readDeleteData(appParams:AppParams,regex:String)(implicit spark:SparkSession,connection:Connection): Unit = {
    val localConfCopy = conf
    val data: RDD[HFileRow] = readLinksWithKeyFilter(localConfCopy,appParams,regex)
    val rows: Array[HFileRow] = data.take(5)
    rows.map(_.toString).foreach(row => print(
      "="*10+
        row+'\n'+
        "="*10
    ))
  }

  def saveDeletePeriodLinksToHFile(appParams:AppParams)(implicit spark:SparkSession): Unit = {
    val localConfCopy = conf
    val regex = ".*~"+{appParams.TIME_PERIOD}+"$"
    val data: RDD[HFileRow] = readLinksWithKeyFilter(localConfCopy,appParams,regex)
    data.sortBy(row => s"${row.key}")
      .flatMap(_.toDeletePeriodHFileEntries(appParams.HBASE_LINKS_COLUMN_FAMILY))
      .saveAsNewAPIHadoopFile(appParams.PATH_TO_LINK_DELETE_PERIOD_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], localConfCopy)
  }


  def saveDeletePeriodEnterpriseToHFile(appParams:AppParams)(implicit spark:SparkSession): Unit = {
    val localConfCopy = conf
    val regex = ".*~"+{appParams.TIME_PERIOD}+"$"
    val data = readEnterprisesWithKeyFilter(localConfCopy,appParams,regex)
    data.sortBy(row => s"${row.key}")
      .flatMap(_.toDeletePeriodHFileEntries(appParams.HBASE_ENTERPRISE_COLUMN_FAMILY))
      .saveAsNewAPIHadoopFile(appParams.PATH_TO_ENTERPRISE_DELETE_PERIOD_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], localConfCopy)
  }



  def saveDeleteLouToHFile(appParams:AppParams)(implicit spark:SparkSession): Unit = {
    val localConfCopy = conf
    val regex = ".*~"+{appParams.TIME_PERIOD}+"~*."
    val data = readLouWithKeyFilter(localConfCopy,appParams,regex)
    data.sortBy(row => s"${row.key}")
      .flatMap(_.toDeletePeriodHFileEntries(appParams.HBASE_LOCALUNITS_COLUMN_FAMILY))
      .saveAsNewAPIHadoopFile(appParams.PATH_TO_LOCALUNITS_DELETE_PERIOD_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], localConfCopy)
  }


  def saveDeleteLinksToHFile(appParams:AppParams,regex:String)(implicit spark:SparkSession): Unit = {
    val localConfCopy = conf
    val data = readLinksWithKeyFilter(localConfCopy,appParams,regex)
    data.sortBy(row => s"${row.key}")
      .flatMap(_.toDeleteHFileEntries(appParams.HBASE_LINKS_COLUMN_FAMILY))
      .saveAsNewAPIHadoopFile(appParams.PATH_TO_LINKS_HFILE_DELETE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], localConfCopy)
  }

  def readLinksWithKeyFilter(confs:Configuration, appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {

    val tableName = s"${appParams.HBASE_LINKS_TABLE_NAMESPACE}:${appParams.HBASE_LINKS_TABLE_NAME}"
    readTableWithKeyFilter(confs, appParams, tableName, regex)

  }

  def readLouWithKeyFilter(confs:Configuration,appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {

    val tableName = s"${appParams.HBASE_LOCALUNITS_TABLE_NAMESPACE}:${appParams.HBASE_LOCALUNITS_TABLE_NAME}"
    readTableWithKeyFilter(confs, appParams, tableName, regex)

  }


  def readEnterprisesWithKeyFilter(confs:Configuration,appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {

    val tableName = s"${appParams.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appParams.HBASE_ENTERPRISE_TABLE_NAME}"
    readTableWithKeyFilter(confs, appParams, tableName, regex)

  }



  def readTableWithKeyFilter(confs:Configuration,appParams:AppParams, tableName:String, regex:String)(implicit spark:SparkSession) = {
    val localConfCopy = confs
    withScanner(localConfCopy,regex,appParams,tableName){
      readKvsFromHBase
    }}
  
  def loadRefreshLinksHFile(implicit connection:Connection, appParams:AppParams) = wrapTransaction(appParams.HBASE_LINKS_TABLE_NAME, Some(appParams.HBASE_LINKS_TABLE_NAMESPACE)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LINKS_HFILE_UPDATE), admin,table,regionLocator)
  }

  def loadDeleteLinksHFile(implicit connection:Connection, appParams:AppParams) = wrapTransaction(appParams.HBASE_LINKS_TABLE_NAME, Some(appParams.HBASE_LINKS_TABLE_NAMESPACE)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LINKS_HFILE_DELETE), admin,table,regionLocator)
  }

  def loadLousDeleteHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(appParams.HBASE_LOCALUNITS_TABLE_NAME,Some(appParams.HBASE_LOCALUNITS_TABLE_NAMESPACE)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LOCALUNITS_DELETE_PERIOD_HFILE), admin,table,regionLocator)
  }

  def loadLinksHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(appParams.HBASE_LINKS_TABLE_NAME, Some(appParams.HBASE_LINKS_TABLE_NAMESPACE)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LINKS_HFILE), admin,table,regionLocator)
  }

  def loadEnterprisesHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(appParams.HBASE_ENTERPRISE_TABLE_NAME,Some(appParams.HBASE_ENTERPRISE_TABLE_NAMESPACE)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_ENTERPRISE_HFILE), admin,table,regionLocator)
  }


  def loadLousHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(appParams.HBASE_LOCALUNITS_TABLE_NAME,Some(appParams.HBASE_LOCALUNITS_TABLE_NAMESPACE)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LOCALUNITS_HFILE), admin,table,regionLocator)
  }


  private def wrapTransaction(tableName:String,nameSpace:Option[String])(action:(Table,Admin) => Unit)(implicit connection:Connection){
    val tn = nameSpace.map(ns => TableName.valueOf(ns, tableName)).getOrElse(TableName.valueOf(tableName))
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table,admin)
    table.close
  }


  private def wrapReadTransaction(tableName:String)(action: String => RDD[HFileRow])(implicit connection:Connection):RDD[HFileRow] = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val admin = connection.getAdmin
    setJob(table)
    val res = action(tableName)
    table.close
    res
  }


  private def setJob(table:Table)(implicit connection:Connection){
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }

  def withScanner(config:Configuration,regex:String, appParams:AppParams, tableName:String)(getResult:(Configuration) => RDD[HFileRow]): RDD[HFileRow] = {
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    setScanner(config,regex,appParams)
    val res = getResult(config)
    unsetScanner(config)
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def readKvsFromHBase(configuration:Configuration)(implicit spark:SparkSession): RDD[HFileRow] =  {
    spark.sparkContext.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map(_._2).map(HFileRow(_))
  }

 private def unsetScanner(config:Configuration) = config.unset(TableInputFormat.SCAN)

  private def setScanner(config:Configuration,regex:String, appParams:AppParams) = {

    val comparator = new RegexStringComparator(regex)
    val filter = new RowFilter(CompareOp.EQUAL, comparator)

    def convertScanToString(scan: Scan): String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      return Base64.encodeBytes(proto.toByteArray())
    }

    val scan = new Scan()
    scan.setFilter(filter)
    val scanStr = convertScanToString(scan)

   config.set(TableInputFormat.SCAN,scanStr)
  }

}
