package connector

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

/**
  *
  */
object HBaseConnector {

  val logger = LoggerFactory.getLogger(getClass)


  private def setJob(table:Table)(implicit connection:Connection) = {
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }


  def loadHFile(pathToHFile:String, tableName:String)(implicit connection:Connection) = wrapTransaction(tableName,None){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(pathToHFile), admin,table,regionLocator)
  }


  def loadHFile(pathToHFile:String, tableName:String,nameSpace:String)(implicit connection:Connection) = wrapTransaction(tableName, if(nameSpace.trim.isEmpty) None else Some(nameSpace)  ){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(pathToHFile), admin,table,regionLocator)
  }


  private def wrapTransaction(tableName:String,nameSpace:Option[String])(action:(Table,Admin) => Unit)(implicit connection:Connection): Unit = {
    val tn = nameSpace.map(ns => TableName.valueOf(ns, tableName)).getOrElse(TableName.valueOf(tableName))
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table,admin)
    admin.close
    table.close
  }


}
