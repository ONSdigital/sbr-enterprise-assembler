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

  def loadHFile(pathToHFile:String, hbaseTableName:String)(implicit connection:Connection) = {
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))
    setJob(table)
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    val admin = connection.getAdmin
    bulkLoader.doBulkLoad(new Path(pathToHFile), admin,table,regionLocator)
    admin.close
    table.close
  }
}
