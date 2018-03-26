package dao.hbase

import java.util

import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FuzzyRowFilter, RegexStringComparator, RowFilter}
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
import org.apache.hadoop.hbase.util.Pair
import spark.extensions.rdd.HBaseDataReader.getKeyValue

import scala.util.Try

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


  def readAll(appParams:AppParams,spark:SparkSession) = {

   val tableName = s"${appParams.HBASE_LINKS_TABLE_NAMESPACE}:${appParams.HBASE_LINKS_TABLE_NAME}"
   conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val rdd  = HBaseDataReader.readKvsFromHBase(spark)

    val recs: Seq[HFileRow] = rdd.collect.toSeq
    print("RECS SIZE: "+recs.size)
   }


  def readWithKeyFilter(appParams:AppParams,regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {

   val tableName = s"${appParams.HBASE_LINKS_TABLE_NAMESPACE}:${appParams.HBASE_LINKS_TABLE_NAME}"
   conf.set(TableInputFormat.INPUT_TABLE, tableName)
    //val regex = "72~LEU~"+{appParams.TIME_PERIOD}+"$"
    setScanner(regex,appParams)
    val rdd: RDD[HFileRow] = HBaseDataReader.readKvsFromHBase(spark)
    rdd
/*    val recs: Iterable[HBaseRow] = rdd.collect
   print("RECS SIZE: "+recs.size)*/
   }



   def readLinksFromHbase(appParams:AppParams)(implicit connection:Connection) = wrapTransaction(appParams.HBASE_LINKS_TABLE_NAME, Some(appParams.HBASE_LINKS_TABLE_NAMESPACE)){ (table, admin) =>

     //val comparator = new RegexStringComparator("~LEU~"+{appParams.TIME_PERIOD}+"$")
     val comparator = new RegexStringComparator(".*(?<!~LEU~"+{appParams.TIME_PERIOD}+")$")//.*(?<!ab)$
     val filter = new RowFilter(CompareOp.EQUAL, comparator)

     val scan = new Scan()
     scan.setFilter(filter)
     val res: Seq[Result] = table.getScanner(scan).iterator().asScala.toList
     val ret: Seq[(String, Array[(String, (String, String))])] = res.map(v => (Bytes.toString(v.getRow),v.rawCells().map(cell => getKeyValue(cell))))
     val fr: Seq[(String, Array[(String, String)])] = ret.map(r => (r._1,r._2.map(e => e._2)))
     fr

   }

/*  def deleteRows(rowIds:List[Array[Byte]])(implicit connection:Connection) = wrapTransaction(HBASE_LINKS_TABLE_NAME, Try(conf.getStrings("hbase.table.links.namespace").head).toOption){ (table, admin) =>
    import collection.JavaConversions._
    table.delete(rowIds.map(rid => new Delete(rid)))
  }

  def deleteRow(rowId:Array[Byte])(implicit connection:Connection) = wrapTransaction(HBASE_LINKS_TABLE_NAME, Try(conf.getStrings("hbase.table.links.namespace").head).toOption){ (table, admin) =>
    table.delete(new Delete(rowId))
  }*/


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
