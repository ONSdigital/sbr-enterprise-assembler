package dao.hbase

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import util.options.ConfigOptions

trait HBaseConnectionManager {

  def withHbaseConnection(action: Connection => Unit) {
    val hbConnection: Connection = ConnectionFactory.createConnection(ConfigOptions.hbaseConfiguration)
    action(hbConnection)
    hbConnection.close()
  }
}
