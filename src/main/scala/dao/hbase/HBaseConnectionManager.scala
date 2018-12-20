package dao.hbase

import global.Configs
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

trait HBaseConnectionManager {

  def withHbaseConnection(action: Connection => Unit) {
    val hbConnection: Connection = ConnectionFactory.createConnection(Configs.conf)
    action(hbConnection)
    hbConnection.close()
  }
}
