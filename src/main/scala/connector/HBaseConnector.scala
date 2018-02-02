package connector

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.mapreduce.Job

/**
  *
  */
object HBaseConnector {

  val tableName = "enterprise"
  val columnFamily = "d"
  val conf: Configuration = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 500)
  conf.set("hbase.zookeeper.quorum", "localhost:2181")

  val connection: Connection = ConnectionFactory.createConnection(conf)



  private def setJob(table:Table) = {
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }

  def loadHFile(path:String) = {
    val table = new HTable(conf, tableName)
    setJob(table)
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(path), table)
  }

}
