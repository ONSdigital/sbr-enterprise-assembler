package dao.hbase

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import util.configuration.AssemblerHBaseConfiguration._

object HBaseConnectionManager {

  def withHbaseConnection(action: Connection => Unit) {

    val hbConnection: Connection = ConnectionFactory.createConnection(hbaseConfiguration)

    action(hbConnection)
    hbConnection.close()
  }
}
