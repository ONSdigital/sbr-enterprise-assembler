package dao.hbase

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import util.options.{Config, ConfigOptions, OptionNames}

trait HBaseConnectionManager {

  def withHbaseConnection(action: Connection => Unit) {

    ConfigOptions.hbaseConfiguration.set(OptionNames.HBaseZookeeperQuorum, Config(OptionNames.HBaseZookeeperQuorum))
    ConfigOptions.hbaseConfiguration.set(OptionNames.HBaseZookeeperClientPort, Config(OptionNames.HBaseZookeeperClientPort))
    ConfigOptions.hbaseConfiguration.set(OptionNames.HFilesPerRegion, Config(OptionNames.HFilesPerRegion))

    val hbConnection: Connection = ConnectionFactory.createConnection(ConfigOptions.hbaseConfiguration)

    action(hbConnection)
    hbConnection.close()
  }
}
