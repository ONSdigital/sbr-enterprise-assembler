package util.configuration

object AssemblerConfiguration {

  val TimePeriod: String = Config(AssemblerOptions.TimePeriod)

  val HBaseLinksTableNamespace: String = Config(AssemblerOptions.HBaseLinksTableNamespace)
  val HBaseLinksTableName: String = Config(AssemblerOptions.HBaseLinksTableName)
  val HBaseLinksColumnFamily: String = Config(AssemblerOptions.HBaseLinksColumnFamily)

  val HBaseLegalUnitsNamespace: String = Config(AssemblerOptions.HBaseLegalUnitsNamespace)
  val HBaseLegalUnitsTableName: String = Config(AssemblerOptions.HBaseLegalUnitsTableName)
  val HBaseLegalUnitsColumnFamily: String = Config(AssemblerOptions.HBaseLegalUnitsColumnFamily)

  val HBaseLocalUnitsNamespace: String = Config(AssemblerOptions.HBaseLocalUnitsNamespace)
  val HBaseLocalUnitsTableName: String = Config(AssemblerOptions.HBaseLocalUnitsTableName)
  val HBaseLocalUnitsColumnFamily: String = Config(AssemblerOptions.HBaseLocalUnitsColumnFamily)

  val HBaseReportingUnitsNamespace: String = Config(AssemblerOptions.HBaseReportingUnitsNamespace)
  val HBaseReportingUnitsTableName: String = Config(AssemblerOptions.HBaseReportingUnitsTableName)
  val HBaseReportingUnitsColumnFamily: String = Config(AssemblerOptions.HBaseReportingUnitsColumnFamily)

  val HBaseEnterpriseTableNamespace: String = Config(AssemblerOptions.HBaseEnterpriseTableNamespace)
  val HBaseEnterpriseTableName: String = Config(AssemblerOptions.HBaseEnterpriseTableName)
  val HBaseEnterpriseColumnFamily: String = Config(AssemblerOptions.HBaseEnterpriseColumnFamily)

  val PathToLinksHfile: String = Config(AssemblerOptions.HBaseLinksFilePath)
  val PathToEnterpriseHFile: String = Config(AssemblerOptions.HBaseEnterpriseFilePath)
  val PathToLocalUnitsHFile: String = Config(AssemblerOptions.HBaseLocalUnitsFilePath)
  val PathToLegalUnitsHFile: String = Config(AssemblerOptions.HBaseLegalUnitsFilePath)
  val PathToReportingUnitsHFile: String = Config(AssemblerOptions.HBaseReportingUnitsFilePath)

  val PathToParquet: String = Config(AssemblerOptions.PathToParquet)
  val CreateParquet: String = Config(AssemblerOptions.CreateParquet)

  val DefaultRegion = ""
  val DefaultPostCode = "XZ9 9XX"

  val PathToGeo: String = Config(AssemblerOptions.PathToGeo)
  val PathToGeoShort: String = Config(AssemblerOptions.PathToGeoShort)

  val PathToPaye: String = Config(AssemblerOptions.PayeFilePath)
  val PathToVat: String = Config(AssemblerOptions.VatFilePath)

  val HiveDBName: String = Config(AssemblerOptions.HiveDBName)
  val HiveTableName: String = Config(AssemblerOptions.HiveTableName)
  val HiveShortTableName: String = Config(AssemblerOptions.HiveShortTableName)

  val newLeusViewName = "NEWLEUS"

  val HBaseZookeeperQuorum = Config(AssemblerOptions.HBaseZookeeperQuorum)
  val HBaseZookeeperPort = Config(AssemblerOptions.HBaseZookeeperClientPort)

  val SequenceFormat = Config(AssemblerOptions.SequenceFormat)
  val SequencePath = Config(AssemblerOptions.SequencePath)
  val SequenceSessionTimeout = Config(AssemblerOptions.SequenceSessionTimeout)
  val SequenceConnectionTimeout = Config(AssemblerOptions.SequenceConnectionTimeout)
  val SequenceURL = Config(AssemblerOptions.SequenceURL)

  val BIFilePath = Config(AssemblerOptions.BIFilePath)

  val DefaultPRN = Config(AssemblerOptions.DefaultPRN)
  val DefaultWorkingProps = Config(AssemblerOptions.DefaultWorkingProps)

  val ApplicationEnvironment = Config(AssemblerOptions.ApplicationEnvironment)

  val HFilesPerRegion = Config(AssemblerOptions.HFilesPerRegion)

  val HBaseRPCTimeout = Config(AssemblerOptions.HBaseRPCTimeout)
  val HBaseClientScannerTimeout = Config(AssemblerOptions.HBaseClientScannerTimeout)
  val HBaseCellsScanned = Config(AssemblerOptions.HBaseCellsScanned)

  val Debug = Config(AssemblerOptions.Debug)

  val Cluster = "cluster"
  val Local = "local"

  def isLocal: Boolean = Config(AssemblerOptions.Environment) == Local
  def inCluster: Boolean = Config(AssemblerOptions.Environment) == Cluster
  def createParquetFile: Boolean = CreateParquet == "true"

}
