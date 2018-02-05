package connector

import global.ApplicationContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

/**
  *
  */
object HBaseConnector {

  import global.ApplicationContext._

  val logger = LoggerFactory.getLogger(getClass)

  val conf: Configuration = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, config.getString("hbase.table.name"))
      conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", config.getInt("hbase.files.per.region"))
  conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeper.url"))

  val connection: Connection = ConnectionFactory.createConnection(conf)



  private def setJob(table:Table) = {
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }

  def loadHFile(pathToHFile:String = config.getString("files.hfile")) = {
    val table: Table = connection.getTable(TableName.valueOf(config.getString("hbase.table.name")))
    setJob(table)
    val bulkLoader = new LoadIncrementalHFiles(conf)
    val regionLocator = connection.getRegionLocator(table.getName)
    val admin = connection.getAdmin
    bulkLoader.doBulkLoad(new Path(pathToHFile), admin,table,regionLocator)
    table.close
  }



  def closeConnection = if(closedConnection) Unit else System.exit(1)


  private def closedConnection: Boolean = {

    def isClosed(waitingMillis: Long): Boolean = if (!connection.isClosed) {
      wait(waitingMillis)
      connection.isClosed
    } else true

    @tailrec
    def tryClosing(checkIntervalSec: Long, totalNoOfAttempts: Int, noOfAttemptsLeft: Int): Boolean = {
      if (connection.isClosed) true
      else if (noOfAttemptsLeft == 0) {
        logger.warn(s"Could not close HBase connection. Attempted $totalNoOfAttempts times with intervals of $checkIntervalSec millis")
        false
      } else {
        connection.close
        if (isClosed(checkIntervalSec)) true
        else {
          logger.info(s"trying closing hbase connection. Attempt ${totalNoOfAttempts - noOfAttemptsLeft} of $totalNoOfAttempts")
          tryClosing(checkIntervalSec, totalNoOfAttempts, noOfAttemptsLeft - 1)
        }
      }
    }


    tryClosing(1000L, 5, 5)
  }

}
