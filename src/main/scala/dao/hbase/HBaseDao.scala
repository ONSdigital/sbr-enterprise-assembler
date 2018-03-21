package dao.hbase

import global.AppParams
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
object HBaseDao {
  import global.Configs._

  val logger = LoggerFactory.getLogger(getClass)


  def loadHFiles(implicit connection:Connection,appParams:AppParams) = {
    loadLinksHFile
    loadEnterprisesHFile
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


}
