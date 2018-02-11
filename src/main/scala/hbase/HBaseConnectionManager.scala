package hbase

import connector.HBaseConnector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.Try

/**
  *
  */
trait ConnectionManager {

  import global.ApplicationContext._

  val logger = LoggerFactory.getLogger(getClass)

   import global.ApplicationContext._

  def connectionManaged(action:(Connection) => Unit) = {
    implicit val hbConnection: Connection = ConnectionFactory.createConnection(conf)
    action(hbConnection)
    closeConnection(hbConnection)
  }

  def closeConnection(hbConnection: Connection) = if(connectionClosed(hbConnection: Connection)) Unit else System.exit(1)


  private def connectionClosed(hbConnection: Connection): Boolean = {

    def isClosed(waitingMillis: Long): Boolean = if (!hbConnection.isClosed) {
      wait(waitingMillis)
      hbConnection.isClosed
    } else true

    @tailrec
    def tryClosing(checkIntervalSec: Long, totalNoOfAttempts: Int, noOfAttemptsLeft: Int): Boolean = {
      if (hbConnection.isClosed) true
      else if (noOfAttemptsLeft == 0) {
        logger.warn(s"Could not close HBase connection. Attempted $totalNoOfAttempts times with intervals of $checkIntervalSec millis")
        false
      } else {
        hbConnection.close
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
