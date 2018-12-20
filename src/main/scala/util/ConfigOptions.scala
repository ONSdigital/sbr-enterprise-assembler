package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object ConfigOptions {

  val hbaseConfiguration: Configuration = HBaseConfiguration.create

  if (Config(OptionNames.HadoopSecurityAuthentication) == "NOT_FOUND")
    hbaseConfiguration.set(OptionNames.HadoopSecurityAuthentication, "kerberos")
  else
    hbaseConfiguration.addResource(Config(OptionNames.HadoopSecurityAuthentication))

  lazy val TimePeriod: String = Config(OptionNames.TimePeriod)

  lazy val HBaseLinksTableNamespace: String = Config(OptionNames.HBaseLinksTableNamespace)
  lazy val HBaseLinksTableName: String = Config(OptionNames.HBaseLinksTableName)
  lazy val HBaseLinksColumnFamily: String = Config(OptionNames.HBaseLinksColumnFamily)

  lazy val HBaseLegalUnitsNamespace: String = Config(OptionNames.HBaseLegalUnitsNamespace)
  lazy val HBaseLegalUnitsTableName: String = Config(OptionNames.HBaseLegalUnitsTableName)
  lazy val HBaseLegalUnitsColumnFamily: String = Config(OptionNames.HBaseLegalUnitsColumnFamily)

  lazy val HBaseLocalUnitsNamespace: String = Config(OptionNames.HBaseLocalUnitsNamespace)
  lazy val HBaseLocalUnitsTableName: String = Config(OptionNames.HBaseLocalUnitsTableName)
  lazy val HBaseLocalUnitsColumnFamily: String = Config(OptionNames.HBaseLocalUnitsColumnFamily)

  lazy val HBaseReportingUnitsNamespace: String = Config(OptionNames.HBaseReportingUnitsNamespace)
  lazy val HBaseReportingUnitsTableName: String = Config(OptionNames.HBaseReportingUnitsTableName)

  lazy val HBaseEnterpriseTableNamespace: String = Config(OptionNames.HBaseEnterpriseTableNamespace)
  lazy val HBaseEnterpriseTableName: String = Config(OptionNames.HBaseEnterpriseTableName)
  lazy val HBaseEnterpriseColumnFamily: String = Config(OptionNames.HBaseEnterpriseColumnFamily)

  lazy val PathToLinksHfile: String = Config(OptionNames.PathToLinksHfile)
  lazy val PathToEnterpriseHFile: String = Config(OptionNames.PathToEnterpriseHFile)
  lazy val PathToLocalUnitsHFile: String = Config(OptionNames.PathToLocalUnitsHFile)
  lazy val PathToLegalUnitsHFile: String = Config(OptionNames.PathToLegalUnitsHFile)
  lazy val PathToReportingUnitsHFile: String = Config(OptionNames.PathToReportingUnitsHFile)

  lazy val PathToParquet: String = Config(OptionNames.PathToParquet)

  lazy val DefaultPRN = "0"
  lazy val DefaultWorkingProps = "0"
  lazy val DefaultRegion = ""
  lazy val DefaultPostCode = "XZ9 9XX"

  lazy val PathToGeo: String = Config(OptionNames.PathToGeo)
  lazy val PathToGeoShort: String = Config(OptionNames.PathToGeoShort)

  lazy val HiveDBName: String = Config(OptionNames.HiveDBName)
  lazy val HiveTableName: String = Config(OptionNames.HiveTableName)
  lazy val HiveShortTableName: String = Config(OptionNames.HiveShortTableName)

  val newLeusViewName = "NEWLEUS"

  val ZookeeperHost = Config(OptionNames.ZookeeperHost)
  val ZookeeperQuorum = Config(OptionNames.ZookeeperQuorum)
  val ZookeeperFormat = Config(OptionNames.ZookeeperFormat)
  val ZookeeperPath = Config(OptionNames.ZookeeperPath)
  val ZookeeperSessionTimeout = Config(OptionNames.ZookeeperConnectionTimeout)
  val ZookeeperConnectionTimeout = Config(OptionNames.ZookeeperConnectionTimeout)

  lazy val ApplicationEnvironment = Config(OptionNames.ApplicationEnvironment)

  lazy val Debug = Config(OptionNames.Debug)

  val inCluster: Boolean = ApplicationEnvironment == OptionNames.inCluster
  val local: Boolean = ApplicationEnvironment == OptionNames.local

//  val PREVIOUS_TIME_PERIOD: String = (TimePeriod.toInt - 1).toString //temp
//  val PATH_TO_LEU_TO_ENT_CSV: String = "src/main/resources/data/LeU_to_ENT_subset.csv"

//  val DEFAULT_GEO_PATH: String = "src/main/resources/data/geo/test-dataset.csv"
//  val DEFAULT_GEO_PATH_SHORT: String = "src/main/resources/data/geo/test_short-dataset.csv"
//
//  val PATH_TO_GEO: String = DEFAULT_GEO_PATH
//  val PATH_TO_GEO_SHORT: String = DEFAULT_GEO_PATH_SHORT

}
