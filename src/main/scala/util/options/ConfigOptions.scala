package util.options

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object ConfigOptions extends Serializable {

  val hbaseConfiguration: Configuration = HBaseConfiguration.create

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
  val CreateParquet: String = Config(OptionNames.CreateParquet)

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

  val SequenceFormat = Config(OptionNames.SequenceFormat)
  val SequencePath = Config(OptionNames.SequencePath)
  val SequenceSessionTimeout = Config(OptionNames.SequenceSessionTimeout)
  val SequenceConnectionTimeout = Config(OptionNames.SequenceConnectionTimeout)
  val SequenceURL = Config(OptionNames.SequenceURL)

  val BIFilePath = Config(OptionNames.BIFilePath)

  val DefaultPRN: String =  Config(OptionNames.DefaultPRN)
  val DefaultWorkingProps = Config(OptionNames.DefaultWorkingProps)

  val ApplicationEnvironment = Config(OptionNames.ApplicationEnvironment)

  val Debug = Config(OptionNames.Debug)

  val inCluster: Boolean = ApplicationEnvironment == OptionNames.inCluster
  val local: Boolean = ApplicationEnvironment == OptionNames.local

}
