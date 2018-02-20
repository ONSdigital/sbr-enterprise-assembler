package connector

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  *
  */
object HBaseConnector {
  import global.Configured._

  val logger = LoggerFactory.getLogger(getClass)


  def loadHFiles(implicit connection:Connection) ={
    loadLinksHFile
    loadEnterprisesHFile
  }


  def loadLinksHFile(implicit connection:Connection) = wrapTransaction(HBASE_LINKS_TABLE_NAME, Try(conf.getStrings("hbase.table.links.namespace").head).toOption){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PATH_TO_LINKS_HFILE), admin,table,regionLocator)
  }

  def loadEnterprisesHFile(implicit connection:Connection) = wrapTransaction(HBASE_ENTERPRISE_TABLE_NAME,Try(conf.getStrings("hbase.table.enterprise.namespace").head).toOption){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PATH_TO_ENTERPRISE_HFILE), admin,table,regionLocator)
  }

/*
  def loadHFile(pathToHFile:String, tableName:String,nameSpace:String)(implicit connection:Connection) = wrapTransaction(tableName, if(nameSpace.trim.isEmpty) None else Some(nameSpace)  ){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(pathToHFile), admin,table,regionLocator)
  }*/


  private def wrapTransaction(tableName:String,nameSpace:Option[String])(action:(Table,Admin) => Unit)(implicit connection:Connection): Unit = {
    val tn = nameSpace.map(ns => TableName.valueOf(ns, tableName)).getOrElse(TableName.valueOf(tableName))
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table,admin)
    admin.close
    table.close
  }


  private def setJob(table:Table)(implicit connection:Connection) = {
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }


}
