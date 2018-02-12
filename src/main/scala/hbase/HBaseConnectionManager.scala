package hbase

import global.ApplicationConfig
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

import scala.annotation.tailrec

/**
  *
  */
trait ConnectionManager  {this:ApplicationConfig =>


  def connectionManaged(action:(Connection) => Unit) = {
    val hbConnection: Connection = ConnectionFactory.createConnection(conf)
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
