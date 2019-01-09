package util.configuration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object AssemblerHBaseConfiguration {

  implicit val hbaseConfiguration: Configuration = HBaseConfiguration.create

  if (Config(AssemblerOptions.HadoopSecurityAuthentication) == "NOT_FOUND")
    hbaseConfiguration.set(AssemblerOptions.HadoopSecurityAuthentication, "kerberos")
  else
    hbaseConfiguration.addResource(Config(AssemblerOptions.HadoopSecurityAuthentication))

  hbaseConfiguration.set(AssemblerOptions.HBaseZookeeperQuorum, AssemblerConfiguration.HBaseZookeeperQuorum)
  hbaseConfiguration.set(AssemblerOptions.HBaseZookeeperClientPort, AssemblerConfiguration.HBaseZookeeperPort)
  hbaseConfiguration.setInt(AssemblerOptions.HFilesPerRegion, AssemblerConfiguration.HFilesPerRegion.toInt)

  hbaseConfiguration.setInt(AssemblerOptions.HFilesPerRegion, AssemblerConfiguration.HFilesPerRegion.toInt)
  hbaseConfiguration.setInt(AssemblerOptions.HBaseRPCTimeout, AssemblerConfiguration.HBaseRPCTimeout.toInt)
  hbaseConfiguration.setInt(AssemblerOptions.HBaseClientScannerTimeout, AssemblerConfiguration.HBaseClientScannerTimeout.toInt)
  hbaseConfiguration.setInt(AssemblerOptions.HBaseCellsScanned, AssemblerConfiguration.HBaseCellsScanned.toInt)

}
