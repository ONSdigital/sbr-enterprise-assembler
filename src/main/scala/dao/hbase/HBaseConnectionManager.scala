package dao.hbase

import global.Configs
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.slf4j.LoggerFactory

/**
  *
  */
trait HBaseConnectionManager {

  val logger = LoggerFactory.getLogger(getClass)

  def withHbaseConnection(action:(Connection) => Unit){
    val hbConnection: Connection = ConnectionFactory.createConnection(Configs.conf)
    action(hbConnection)
    hbConnection.close
  }


}
