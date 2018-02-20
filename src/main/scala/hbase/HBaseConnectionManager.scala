package hbase

import global.Configured
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.slf4j.LoggerFactory

/**
  *
  */
trait ConnectionManagement  {

  val logger = LoggerFactory.getLogger(getClass)

  def withHbaseConnection(action:(Connection) => Unit){
    val hbConnection: Connection = ConnectionFactory.createConnection(Configured.conf)
    action(hbConnection)
    hbConnection.close
  }


}
