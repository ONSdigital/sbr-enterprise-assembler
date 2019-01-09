package util.configuration

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.cli.{BasicParser, CommandLine, Option, Options, ParseException}
import util.BuildInfo

/**
  * Parse the command line arguments and add them to system properties.
  *
  * This must be called before the Config object is created so it can be
  * merged with the properties file items or override them as required.
  *
  */
object CommandLineParser {

  implicit val options: Options = new Options

  val help: Option = new Option("h", "help", false, "print this message")
  help.setRequired(false)

  val version: Option = new Option("v", "version", false, "show version information and exit")
  version.setRequired(false)

  AppOptions(options, shortOpt = "e", required = false, hasArg = true,
    "local | cluster", "environment", "local or cluster environment",
    AssemblerOptions.Environment)

  AppOptions(options, shortOpt = "quorum", required = false, hasArg = true,
    "HOST[,HOST...]", "zookeeper-quorum", "host[,host...] for the HBase zookeeper instance(s)",
    AssemblerOptions.HBaseZookeeperQuorum)

  AppOptions(options, shortOpt = "port", required = false, hasArg = true,
    "PORT", "zookeeper-port", "port for the HBase zookeeper instance(s)",
    AssemblerOptions.HBaseZookeeperClientPort)

  AppOptions(options, shortOpt = "seq", required = false, hasArg = true,
    "HOST:PORT[,HOST:PORT...]", "seq-url", "a list of HOST:PORT[,HOST:PORT...] for the Zookeeper sequence number generator hosts(s)",
    AssemblerOptions.SequenceURL)

  AppOptions(options, shortOpt = "etn", required = false, hasArg = true,
    "TABLE NAME", "enterprise-table-name", "HBase Enterprise table name",
    AssemblerOptions.HBaseEnterpriseTableName)

  AppOptions(options, shortOpt = "ens", required = false, hasArg = true,
    "NAMESPACE", "enterprise-table-namespace", "enterprise table namespace",
    AssemblerOptions.HBaseEnterpriseTableNamespace)

  AppOptions(options, shortOpt = "ecf", required = false, hasArg = true,
    "COLUMN FAMILY", "enterprise-column-family", "enterprise column family",
    AssemblerOptions.HBaseEnterpriseColumnFamily)

  AppOptions(options, shortOpt = "efp", required = false, hasArg = true,
    argName = "FILE PATH", longOpt = "enterprise-file-path", desc = "enterprise file path",
    AssemblerOptions.HBaseEnterpriseFilePath)

  AppOptions(options, shortOpt = "ltn", required = false, hasArg = true,
    argName = "TABLE NAME", longOpt = "links-table-name", desc = "HBase links table name",
    AssemblerOptions.HBaseLinksTableName)

  AppOptions(options, "lns", required = false, hasArg = true,
    "NAMESPACE", "links-table-namespace", "links table namespace",
    AssemblerOptions.HBaseLinksTableNamespace)

  AppOptions(options, "lcf", required = false, hasArg = true,
    "COLUMN FAMILY", "links-column-family", "links column family",
    AssemblerOptions.HBaseLinksColumnFamily)

  AppOptions(options, "lfp", required = false, hasArg = true,
    "FILE PATH", "links-file-path", "links file path",
    AssemblerOptions.HBaseLinksFilePath)

  AppOptions(options, "letn", required = false, hasArg = true,
    "TABLE NAME", "legal-table-name", "HBase legal table name",
    AssemblerOptions.HBaseLegalUnitsTableName)

  AppOptions(options, "lens", required = false, hasArg = true,
    "NAMESPACE", "legal-table-namespace", "legal table namespace",
    AssemblerOptions.HBaseLegalUnitsNamespace)

  AppOptions(options, "lecf", required = false, hasArg = true,
    "COLUMN FAMILY", "legal-column-family", "legal column family",
    AssemblerOptions.HBaseLegalUnitsColumnFamily)

  AppOptions(options, "lefp", required = false, hasArg = true,
    "FILE PATH", "legal-file-path", "legal file path",
    AssemblerOptions.HBaseLegalUnitsFilePath)

  AppOptions(options, "lotn", required = false, hasArg = true,
    "TABLE NAME", "local-table-name", "HBase local table name",
    AssemblerOptions.HBaseLocalUnitsTableName)

  AppOptions(options, "lons", required = false, hasArg = true,
    "NAMESPACE", "local-table-namespace", "local table namespace",
    AssemblerOptions.HBaseLocalUnitsNamespace)

  AppOptions(options, "locf", required = false, hasArg = true,
    "COLUMN FAMILY", "local-column-family", "local table column family",
    AssemblerOptions.HBaseLocalUnitsColumnFamily)

  AppOptions(options, "lofp", required = false, hasArg = true,
    "FILE PATH", "local-file-path", "local file path",
    AssemblerOptions.HBaseLocalUnitsFilePath)

  AppOptions(options, "retn", required = false, hasArg = true,
    "TABLE NAME", "reporting-table-name", "HBase reporting table name",
    AssemblerOptions.HBaseReportingUnitsTableName)

  AppOptions(options, "rens", required = false, hasArg = true,
    "NAMESPACE", "reporting-table-namespace", "reporting table namespace",
    AssemblerOptions.HBaseReportingUnitsNamespace)

  AppOptions(options, "recf", required = false, hasArg = true,
    "COLUMN FAMILY", "reporting-column-family", "reporting table column family",
    AssemblerOptions.HBaseReportingUnitsColumnFamily)

  AppOptions(options, "refp", required = false, hasArg = true,
    "FILE PATH", "reporting-file-path", "reporting file path",
    AssemblerOptions.HBaseReportingUnitsFilePath)

  AppOptions(options, "tp", required = false, hasArg = true,
    "TIME PERIOD", "time-period", "time period",
    AssemblerOptions.TimePeriod)

  AppOptions(options, "paye", required = false, hasArg = true,
    "FILE PATH", "paye-file-path", "PAYE file path",
    AssemblerOptions.PayeFilePath)

  AppOptions(options, "vat", required = false, hasArg = true,
    "FILE PATH", "vat-file-path", "VAT file path",
    AssemblerOptions.VatFilePath)

  AppOptions(options, "geo", required = false, hasArg = true,
    "FILE PATH", "path-to-geo", "GEO file path",
    AssemblerOptions.PathToGeo)

  AppOptions(options, "geoShort", required = false, hasArg = true,
    "FILE PATH", "path-to-geo-short", "GEO short file path",
    AssemblerOptions.PathToGeoShort)

  AppOptions(options, "hiveDB", required = false, hasArg = true,
    "DATABASE NAME", "hive-db-name", "Hive database name",
    AssemblerOptions.HiveDBName)

  AppOptions(options, "hiveTable", required = false, hasArg = true,
    "TABLE NAME", "hive-table-name", "Hive table name",
    AssemblerOptions.HiveTableName)

  AppOptions(options, "hiveShortTable", required = false, hasArg = true,
    "TABLE NAME", "hive-short-table-name", "Hive short table name",
    AssemblerOptions.HiveShortTableName)

  AppOptions(options, "bi", required = false, hasArg = true,
    "FILE PATH", "bi-file-path", "the BI JSON input file path",
    AssemblerOptions.BIFilePath)

  AppOptions(options, "parquet", required = false, hasArg = true,
    "FILE PATH", "parquet-file-path", "the parquet output file path",
    AssemblerOptions.PathToParquet)

  AppOptions(options, "create", required = false, hasArg = false,
    "create the Parquet file", "create-parquet", "create the parquet from the JSON file",
    AssemblerOptions.CreateParquet)

  def apply(args: Array[String]): Unit = {

    import org.apache.commons.cli.HelpFormatter

    if (checkForHelp(args)) {
      val formatter = new HelpFormatter
      formatter.setWidth(100)

      val header = "\n(Re)Run the SBR Enterprise Assembler\n\n"

      formatter.printHelp(s"${BuildInfo.name}", header, options, null, true)
      System.exit(0)
    }

    if (checkForVersion(args)) {
      val compiled = new Date(BuildInfo.buildTime)
      val format = new SimpleDateFormat("dd MMM yyyy 'at' HH:mm:ss a").format(compiled)

      println(s"\n${BuildInfo.name} v${BuildInfo.version}.b${BuildInfo.buildInfoBuildNumber}, built on $format")
      println(s"Spark Version: ${BuildInfo.spark}")
      println(s"HBase Version: ${BuildInfo.hbase}")

      System.exit(0)
    }

    val parser = new BasicParser

    try {

      val line: CommandLine = parser.parse(options, args)

      updateEnvironment(line)

    } catch {
      case exp: ParseException =>
        // oops, something went wrong
        println(s"Options Error: ${exp.getMessage}")
        System.exit(1)
    }
  }

  /**
    * The environment overrides values stored in the properties file
    *
    * @param line the option commandline
    */
  def updateEnvironment(line: CommandLine): Unit = {

    for (name <- line.getOptions) {
      val v: AppOptions = AppOptions.opt(name.getOpt)
      if (name.getValue == null)
        System.setProperty(v.optionName, "true")
      else
        System.setProperty(v.optionName, name.getValue)
    }
  }

  def checkForVersion(args: Array[String]): Boolean = {
    var hasVersion = false
    val options = new Options
    try {
      options.addOption(version)
      val parser = new BasicParser
      val cmd = parser.parse(options, args)
      if (cmd.hasOption(version.getOpt)) hasVersion = true
    } catch {
      case e: ParseException =>
    }
    hasVersion
  }

  def checkForHelp(args: Array[String]): Boolean = {
    var hasHelp = false
    val options = new Options
    try {
      options.addOption(help)
      val parser = new BasicParser
      val cmd = parser.parse(options, args)
      if (cmd.hasOption(help.getOpt)) hasHelp = true
    } catch {
      case e: ParseException =>
    }
    hasHelp
  }
}

class AppOptions(shortOpt: String, required: Boolean, hasArg: Boolean, argName: String,
                 longOpt: String, desc: String, val optionName: String)(implicit val options: Options) {

  val option: Option = new Option(shortOpt, longOpt, hasArg, desc)
  option.setRequired(required)
  options.addOption(option)
}

object AppOptions {

  var opt: Map[String, AppOptions] = Map()

  def apply(implicit options: Options, shortOpt: String, required: Boolean, hasArg: Boolean, argName: String,
            longOpt: String, desc: String, optionName: String): Option = {

    val v = new AppOptions(shortOpt, required, hasArg, argName, longOpt, desc, optionName)
    opt += shortOpt -> v
    v.option
  }

}
