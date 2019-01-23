package util.configuration

object AssemblerConfiguration {

  val TimePeriod: String = Config(AssemblerOptions.TimePeriod)

  val HBaseLinksTableNamespace: String = Config(AssemblerOptions.HBaseLinksTableNamespace)
  val HBaseLinksTableName: String = Config(AssemblerOptions.HBaseLinksTableName)
  val HBaseLinksColumnFamily: String = Config(AssemblerOptions.HBaseLinksColumnFamily)

  val HBaseAdminDataTableNamespace: String = Config(AssemblerOptions.HBaseAdminDataTableNamespace)
  val HBaseAdminDataTableName: String = Config(AssemblerOptions.HBaseAdminDataTableName)
  val HBaseAdminDataColumnFamily: String = Config(AssemblerOptions.HBaseAdminDataColumnFamily)

  val HBaseEnterpriseTableNamespace: String = Config(AssemblerOptions.HBaseEnterpriseTableNamespace)
  val HBaseEnterpriseTableName: String = Config(AssemblerOptions.HBaseEnterpriseTableName)
  val HBaseEnterpriseColumnFamily: String = Config(AssemblerOptions.HBaseEnterpriseColumnFamily)

  val PathToLinksHFile: String = Config(AssemblerOptions.HBaseLinksFilePath)
  val PathToAdminDataHFile: String = Config(AssemblerOptions.HBaseAdminDataFilePath)
  val PathToRegionHFile: String = Config(AssemblerOptions.HBaseRegionFilePath)
  val PathToEmploymentHFile: String = Config(AssemblerOptions.HBaseEmploymentFilePath)
  val PathToEnterpriseHFile: String = Config(AssemblerOptions.HBaseEnterpriseFilePath)

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

  val HFilesCreateSuccessFiles = Config(AssemblerOptions.HFilesCreateSuccessFiles)

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
