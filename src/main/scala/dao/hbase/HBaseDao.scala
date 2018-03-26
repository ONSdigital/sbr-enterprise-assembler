package dao.hbase

import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import spark.extensions.rdd.HBaseDataReader
import spark.extensions.rdd.HBaseDataReader.getKeyValue

/**
  *
  */
object HBaseDao {
  import global.Configs._

  import collection.JavaConverters._

  val logger = LoggerFactory.getLogger(getClass)



  def loadHFiles(implicit connection:Connection,appParams:AppParams) = {
    loadLinksHFile
    loadEnterprisesHFile
  }

//!
  def readWithKeyFilter(appParams:AppParams,regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {

   val tableName = s"${appParams.HBASE_LINKS_TABLE_NAMESPACE}:${appParams.HBASE_LINKS_TABLE_NAME}"
   conf.set(TableInputFormat.INPUT_TABLE, tableName)
    //val regex = "72~LEU~"+{appParams.TIME_PERIOD}+"$"
    setScanner(regex,appParams)
    val data = HBaseDataReader.readKvsFromHBase(spark)
    unsetScanner
    data
   }

  def loadRefreshHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(appParams.HBASE_LINKS_TABLE_NAME, Some(appParams.HBASE_LINKS_TABLE_NAMESPACE)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LINKS_HFILE_UPDATE), admin,table,regionLocator)
  }

  def loadDeleteHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(appParams.HBASE_LINKS_TABLE_NAME, Some(appParams.HBASE_LINKS_TABLE_NAMESPACE)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LINKS_HFILE_DELETE), admin,table,regionLocator)
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

  private def wrapTransaction(tableName:String,nameSpace:Option[String])(action:(Table,Admin) => Unit)(implicit connection:Connection){
    val tn = nameSpace.map(ns => TableName.valueOf(ns, tableName)).getOrElse(TableName.valueOf(tableName))
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table,admin)
    admin.close
    table.close
  }


  private def setJob(table:Table)(implicit connection:Connection){
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }

 def unsetScanner = conf.unset(TableInputFormat.SCAN)

 def setScanner(regex:String, appParams:AppParams) = {

    val comparator = new RegexStringComparator(regex)
    val filter = new RowFilter(CompareOp.EQUAL, comparator)

    def convertScanToString(scan: Scan): String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      return Base64.encodeBytes(proto.toByteArray())
    }

    val scan = new Scan()
    scan.setFilter(filter)
    val scanStr = convertScanToString(scan)

    conf.set(TableInputFormat.SCAN,scanStr)
  }

}
