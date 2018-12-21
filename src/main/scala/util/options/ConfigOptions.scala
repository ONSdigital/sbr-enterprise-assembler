package util.options

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object ConfigOptions extends Serializable {

  val hbaseConfiguration: Configuration = HBaseConfiguration.create

  hbaseConfiguration.setInt(OptionNames.HFilesPerRegion, 500)
  hbaseConfiguration.setInt("hbase.rpc.timeout", 360000)
  hbaseConfiguration.setInt("hbase.client.scanner.timeout.period", 360000)
  hbaseConfiguration.setInt("hbase.cells.scanned.per.heartbeat.check", 60000)

  if (Config(OptionNames.HadoopSecurityAuthentication) == "NOT_FOUND")
    hbaseConfiguration.set(OptionNames.HadoopSecurityAuthentication, "kerberos")
  else
    hbaseConfiguration.addResource(Config(OptionNames.HadoopSecurityAuthentication))

  val TimePeriod: String = Config(OptionNames.TimePeriod)

  val HBaseLinksTableNamespace: String = Config(OptionNames.HBaseLinksTableNamespace)
  val HBaseLinksTableName: String = Config(OptionNames.HBaseLinksTableName)
  val HBaseLinksColumnFamily: String = Config(OptionNames.HBaseLinksColumnFamily)

  val HBaseLegalUnitsNamespace: String = Config(OptionNames.HBaseLegalUnitsNamespace)
  val HBaseLegalUnitsTableName: String = Config(OptionNames.HBaseLegalUnitsTableName)
  val HBaseLegalUnitsColumnFamily: String = Config(OptionNames.HBaseLegalUnitsColumnFamily)

  val HBaseLocalUnitsNamespace: String = Config(OptionNames.HBaseLocalUnitsNamespace)
  val HBaseLocalUnitsTableName: String = Config(OptionNames.HBaseLocalUnitsTableName)
  val HBaseLocalUnitsColumnFamily: String = Config(OptionNames.HBaseLocalUnitsColumnFamily)

  val HBaseReportingUnitsNamespace: String = Config(OptionNames.HBaseReportingUnitsNamespace)
  val HBaseReportingUnitsTableName: String = Config(OptionNames.HBaseReportingUnitsTableName)
  val HBaseReportingUnitsColumnFamily: String = Config(OptionNames.HBaseReportingUnitsColumnFamily)

  val HBaseEnterpriseTableNamespace: String = Config(OptionNames.HBaseEnterpriseTableNamespace)
  val HBaseEnterpriseTableName: String = Config(OptionNames.HBaseEnterpriseTableName)
  val HBaseEnterpriseColumnFamily: String = Config(OptionNames.HBaseEnterpriseColumnFamily)

  val PathToLinksHfile: String = Config(OptionNames.HBaseLinksFilePath)
  val PathToEnterpriseHFile: String = Config(OptionNames.HBaseEnterpriseFilePath)
  val PathToLocalUnitsHFile: String = Config(OptionNames.HBaseLocalUnitsFilePath)
  val PathToLegalUnitsHFile: String = Config(OptionNames.HBaseLegalUnitsFilePath)
  val PathToReportingUnitsHFile: String = Config(OptionNames.HBaseReportingUnitsFilePath)

  val PathToParquet: String = Config(OptionNames.PathToParquet)

  val DefaultRegion = ""
  val DefaultPostCode = "XZ9 9XX"

  val PathToGeo: String = Config(OptionNames.PathToGeo)
  val PathToGeoShort: String = Config(OptionNames.PathToGeoShort)

  val PathToPaye: String = Config(OptionNames.PayeFilePath)
  val PathToVat: String = Config(OptionNames.VatFilePath)

  val HiveDBName: String = Config(OptionNames.HiveDBName)
  val HiveTableName: String = Config(OptionNames.HiveTableName)
  val HiveShortTableName: String = Config(OptionNames.HiveShortTableName)

  val newLeusViewName = "NEWLEUS"

  val HBaseZookeeperQuorum = Config(OptionNames.HBaseZookeeperQuorum)
  val HBaseZookeeperPort = Config(OptionNames.HBaseZookeeperClientPort)

  val ZookeeperFormat = Config(OptionNames.ZookeeperFormat)
  val ZookeeperPath = Config(OptionNames.ZookeeperPath)
  val ZookeeperSessionTimeout = Config(OptionNames.ZookeeperConnectionTimeout)
  val ZookeeperConnectionTimeout = Config(OptionNames.ZookeeperConnectionTimeout)
  val ZookeeperUrl = Config(OptionNames.ZooKeeperUrl)

  val PathToJSON = Config(OptionNames.PathToJSON)

  val DefaultPRN: String =  Config(OptionNames.DefaultPRN)
  val DefaultWorkingProps = Config(OptionNames.DefaultWorkingProps)

  val ApplicationEnvironment = Config(OptionNames.ApplicationEnvironment)

  val Debug = Config(OptionNames.Debug)

  val inCluster: Boolean = ApplicationEnvironment == OptionNames.inCluster
  val local: Boolean = ApplicationEnvironment == OptionNames.local

  val HFilesPerRegion: String = OptionNames.HFilesPerRegion

  //  val PREVIOUS_TIME_PERIOD: String = (TimePeriod.toInt - 1).toString //temp
  //  val PATH_TO_LEU_TO_ENT_CSV: String = "src/main/resources/data/LeU_to_ENT_subset.csv"

  //  val DEFAULT_GEO_PATH: String = "src/main/resources/data/geo/test-dataset.csv"
  //  val DEFAULT_GEO_PATH_SHORT: String = "src/main/resources/data/geo/test_short-dataset.csv"
  //
  //  val PATH_TO_GEO: String = DEFAULT_GEO_PATH
  //  val PATH_TO_GEO_SHORT: String = DEFAULT_GEO_PATH_SHORT

}
