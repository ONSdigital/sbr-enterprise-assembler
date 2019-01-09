package util.configuration

protected object AssemblerOptions {

  val TimePeriod: String = "app.timePeriod"
  val ApplicationEnvironment = "app.environment"
  val Debug = "app.debug"

  val HadoopSecurityAuthentication: String = "hadoop.security.authentication"

  val HBaseLinksTableNamespace: String = "app.links.namespace"
  val HBaseLinksTableName: String = "app.links.tableName"
  val HBaseLinksColumnFamily: String = "app.links.columnFamily"
  val HBaseLinksFilePath: String = "app.links.filePath"

  val HBaseLegalUnitsNamespace: String = "app.legal.namespace"
  val HBaseLegalUnitsTableName: String = "app.legal.tableName"
  val HBaseLegalUnitsColumnFamily: String = "app.legal.columnFamily"
  val HBaseLegalUnitsFilePath: String = "app.legal.filePath"

  val HBaseLocalUnitsNamespace: String = "app.local.namespace"
  val HBaseLocalUnitsTableName: String = "app.local.tableName"
  val HBaseLocalUnitsColumnFamily: String = "app.local.columnFamily"
  val HBaseLocalUnitsFilePath: String = "app.local.filePath"

  val HBaseReportingUnitsNamespace: String = "app.reporting.namespace"
  val HBaseReportingUnitsTableName: String = "app.reporting.tableName"
  val HBaseReportingUnitsColumnFamily: String = "app.reporting.columnFamily"
  val HBaseReportingUnitsFilePath: String = "app.reporting.filePath"

  val HBaseEnterpriseTableNamespace: String = "app.enterprise.namespace"
  val HBaseEnterpriseTableName: String = "app.enterprise.tableName"
  val HBaseEnterpriseColumnFamily: String = "app.enterprise.columnFamily"
  val HBaseEnterpriseFilePath: String = "app.enterprise.filePath"

  val PathToParquet: String = "app.parquetFilePath"
  val CreateParquet: String = "app.createParquetFile"

  val PathToGeo: String = "app.geo.pathToGeo"
  val PathToGeoShort: String = "app.geo.pathToGeoShort"
  val DefaultGeoPath: String = "app.geo.default"
  val DefaultGeoPathShort: String = "app.geo.defaultShort"

  val HiveDBName: String = "app.hive.hiveDBName"
  val HiveTableName: String = "app.hive.hiveTablename"
  val HiveShortTableName: String = "app.hive.hiveShortTablename"

  val PayeFilePath: String = "app.payeFilePath"
  val VatFilePath: String = "app.vatFilePath"

  val SequenceURL = "app.sequence.url"
  val SequenceFormat = "app.sequence.resultFormat"
  val SequencePath = "app.sequence.path"
  val SequenceSessionTimeout = "app.sequence.sessionTimeout"
  val SequenceConnectionTimeout = "app.sequence.connectionTimeout"

  val HBaseZookeeperQuorum = "hbase.zookeeper.quorum"
  val HBaseZookeeperClientPort = "hbase.zookeeper.property.clientPort"
  val HFilesPerRegion = "hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily"
  val HBaseRPCTimeout = "hbase.rpc.timeout"
  val HBaseClientScannerTimeout = "hbase.client.scanner.timeout.period"
  val HBaseCellsScanned = "hbase.cells.scanned.per.heartbeat.check"

  val Environment = "app.environment"

  val BIFilePath = "app.BIFilePath"
  val DefaultPRN = "app.defaultPRN"
  val DefaultWorkingProps = "app.defaultWorkingProps"
}
