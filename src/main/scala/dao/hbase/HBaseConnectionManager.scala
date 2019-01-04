package dao.hbase

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import util.options.ConfigOptions.hbaseConfiguration
import util.options.{Config, ConfigOptions, OptionNames}

trait HBaseConnectionManager {

  def withHbaseConnection(action: Connection => Unit) {

    hbaseConfiguration.set(OptionNames.HBaseZookeeperQuorum, Config(OptionNames.HBaseZookeeperQuorum))
    hbaseConfiguration.set(OptionNames.HBaseZookeeperClientPort, Config(OptionNames.HBaseZookeeperClientPort))
    hbaseConfiguration.setInt(OptionNames.HFilesPerRegion, Config.getInt(OptionNames.HFilesPerRegion))

    hbaseConfiguration.setInt(OptionNames.HFilesPerRegion, Config.getInt(OptionNames.HFilesPerRegion))
    hbaseConfiguration.setInt(OptionNames.HBaseRPCTimeout, Config.getInt(OptionNames.HBaseRPCTimeout))
    hbaseConfiguration.setInt(OptionNames.HBaseClientScannerTimeout, Config.getInt(OptionNames.HBaseClientScannerTimeout))
    hbaseConfiguration.setInt(OptionNames.HBaseCellsScanned, Config.getInt(OptionNames.HBaseCellsScanned))

    val hbConnection: Connection = ConnectionFactory.createConnection(ConfigOptions.hbaseConfiguration)

    action(hbConnection)
    hbConnection.close()
  }
}
