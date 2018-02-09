package connector

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.Try

/**
  *
  */
object HBaseConnector {

  import global.ApplicationContext._

  val logger = LoggerFactory.getLogger(getClass)

  def getIntValue(resourceKey:String, defaultVal:Int) = Try{config.getInt(resourceKey)}.getOrElse(defaultVal)
  def getStrValue(resourceKey:String, defaultVal:String): String = Try{config.getString(resourceKey)}.getOrElse(defaultVal)


  val conf: Configuration = HBaseConfiguration.create()
  Try{config.getString("hbase.kerberos.config")}.map(conf.addResource).getOrElse(logger.info("no config resource for kerberos specified"))
  Try{config.getString("hbase.local.path.config")}.map(conf.addResource).getOrElse {
                logger.info("no config resource for hbase specified. Default configs will be used")
                conf.set("hbase.zookeeper.quorum", config.getString("hbase.local.zookeper.url"))
                conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", config.getInt("hbase.local.files.per.region"))
             }


  val connection: Connection = ConnectionFactory.createConnection(conf)


  private def setJob(table:Table) = {
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }

  def loadHFile(pathToHFile:String = PATH_TO_HFILE)() = {

    val table: Table = connection.getTable(TableName.valueOf(config.getString("hbase.local.table.name")))
    setJob(table)
    val bulkLoader = new LoadIncrementalHFiles(conf)
    val regionLocator = connection.getRegionLocator(table.getName)
    val admin = connection.getAdmin
    bulkLoader.doBulkLoad(new Path(pathToHFile), admin,table,regionLocator)
    table.close
    admin.close
  }



  def closeConnection = if(connectionClosed) Unit else System.exit(1)


  private def connectionClosed: Boolean = {

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
