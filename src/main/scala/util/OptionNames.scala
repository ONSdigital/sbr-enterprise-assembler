package util

object OptionNames extends Serializable {

  val TimePeriod: String = "app.timePeriod"
  val ApplicationEnvironment = "app.environment"
  val Debug = "app.debug"

  val HadoopSecurityAuthentication: String =  "hadoop.security.authentication"
  val HBaseLinksTableNamespace: String = "app.links.namespace"
  val HBaseLinksTableName: String = "app.links.tableName"
  val HBaseLinksColumnFamily: String = "app.links.columnFamily"

  val HBaseLegalUnitsNamespace: String = "app.legal.namespace"
  val HBaseLegalUnitsTableName: String = "app.legal.tableName"
  val HBaseLegalUnitsColumnFamily: String = "app.legal.columnFamily"

  val HBaseLocalUnitsNamespace: String = "app.local.namespace"
  val HBaseLocalUnitsTableName: String = "app.local.tableName"
  val HBaseLocalUnitsColumnFamily: String = "app.local.columnFamily"

  val HBaseReportingUnitsNamespace: String = "app.reporting.namespace"
  val HBaseReportingUnitsTableName: String = "app.reporting.tableName"
  val HBaseReportingUnitsColumnFamily: String = "app.reporting.columnFamily"

  val HBaseEnterpriseTableNamespace: String = "app.enterprise.namespace"
  val HBaseEnterpriseTableName: String = "app.enterprise.tableName"
  val HBaseEnterpriseColumnFamily: String = "app.enterprise.columnFamily"

  val PathToLinksHfile: String = "app.links.filePath"
  val PathToEnterpriseHFile: String = "app.enterprise.filePath"
  val PathToLocalUnitsHFile: String = "app.local.filePath"
  val PathToLegalUnitsHFile: String = "app.legal.filePath"
  val PathToReportingUnitsHFile: String = "app.reporting.filePath"

  val PathToParquet: String = "app.parquetFilePath"

  val PathToGeo: String = "app.geo.pathToGeo"
  val PathToGeoShort: String = "app.geo.pathToGeoShort"
  val DefaultGeoPath: String = "app.geo.default"
  val DefaultGeoPathShort: String = "app.geo.defaultShort"

  val HiveDBName: String = "app.hive.hiveDBName"
  val HiveTableName: String = "app.hive.hiveTablename"
  val HiveShortTableName: String = "app.hive.hiveShortTablename"

  val PayeFilePath: String = "app.payeFilePath"
  val VatFilePath: String = "app.vatFilePath"

  val ZookeeperHost = "app.zookeeper.host"
  val ZookeeperQuorum = "hbase.zookeeper.quorum"
  val ZookeeperFormat = "app.zookeeper.resultFormat"
  val ZookeeperPath = "app.zookeeper.path"
  val ZookeeperSessionTimeout = "app.zookeeper.sessionTimeoutSec"
  val ZookeeperConnectionTimeout = "app.zookeeper.connectionTimeoutSec"

  val Environment = "app.environment"
  val inCluster = "cluster"
  val local = "cluster"

}
