package util.options

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Option, OptionGroup, Options, ParseException}

/**
  * Parse the command line arguments and add them to system properties.
  *
  * This must be called before the Config object is created so it can be
  * merged with the properties file items or override them as required.
  *
  */
object CommandLineParser {

  implicit val options: Options = new Options

  val help: Option = Option.builder("h").required(false)
    .hasArg(false)
    .longOpt("help")
    .desc("Print this message.")
    .build

  val version: Option = Option.builder("v")
    .required(false)
    .hasArg(false)
    .longOpt("version")
    .desc("Show version information and exit")
    .build

  val debug: Option = Option.builder("d")
    .required(false)
    .hasArg(false)
    .longOpt("debug")
    .desc("Switch debugging on")
    .build

  val environment: Option = Option.builder("e")
    .required(false)
    .hasArg(true)
    .longOpt("environment")
    .desc("local | cluster")
    .build

  options.addOption(help)
  options.addOption(debug)
  options.addOption(environment)

  AppOptions(options, shortOpt = "quorum", required = true, hasArg = true,
    "HOST[,HOST...]", "zookeeper-quorum", "host[,host...] for the HBase zookeeper instance(s)",
    OptionNames.HBaseZookeeperQuorum)

  AppOptions(options, shortOpt = "port", required = true, hasArg = true,
    "PORT", "zookeeper-port", "port for the HBase zookeeper instance(s)",
    OptionNames.HBaseZookeeperClientPort)

  AppOptions(options, shortOpt = "etn", required = false, hasArg = true,
    "TABLE NAME", "enterprise-table-name", "HBase Enterprise table name",
    OptionNames.HBaseEnterpriseTableName)

  AppOptions(options, shortOpt = "ens", required = false, hasArg = true,
    "NAMESPACE", "enterprise-table-namespace", "Enterprise table namespace",
    OptionNames.HBaseEnterpriseTableNamespace)

  AppOptions(options, shortOpt = "ecf", required = false, hasArg = true,
    "COLUMN FAMILY", "enterprise-column-family", "Enterprise column family",
    OptionNames.HBaseEnterpriseColumnFamily)

  AppOptions(options, shortOpt = "efp", required = false, hasArg = true,
    argName = "FILE PATH", longOpt = "enterprise-file-path", desc = "Enterprise file path",
    OptionNames.HBaseEnterpriseFilePath)

  AppOptions(options, shortOpt = "ltn", required = false, hasArg = true,
    argName = "TABLE NAME", longOpt = "links-table-name", desc = "HBase Links table name",
    OptionNames.HBaseLinksTableName)

  AppOptions(options, "lns", required = false, hasArg = true,
    "NAMESPACE", "links-table-namespace", "Links table namespace",
    OptionNames.HBaseLinksTableNamespace)

  AppOptions(options, "lcf", required = false, hasArg = true,
    "COLUMN FAMILY", "links-column-family", "Links column family",
    OptionNames.HBaseLinksColumnFamily)

  AppOptions(options, "lfp", required = false, hasArg = true,
    "FILE PATH", "links-file-path", "Links file path",
    OptionNames.HBaseLinksFilePath)

  AppOptions(options, "letn", required = false, hasArg = true,
    "TABLE NAME", "legal-table-name", "HBase Legal table name",
    OptionNames.HBaseLegalUnitsTableName)

  AppOptions(options, "lens", required = false, hasArg = true,
    "NAMESPACE", "legal-table-namespace", "Legal table namespace",
    OptionNames.HBaseLegalUnitsNamespace)

  AppOptions(options, "lecf", required = false, hasArg = true,
    "COLUMN FAMILY", "legal-column-family", "Legal column family",
    OptionNames.HBaseLegalUnitsColumnFamily)

  AppOptions(options, "lefp", required = false, hasArg = true,
    "FILE PATH", "legal-file-path", "Legal file path",
    OptionNames.HBaseLegalUnitsFilePath)

  AppOptions(options, "lotn", required = false, hasArg = true,
    "TABLE NAME", "local-table-name", "HBase Local table name",
    OptionNames.HBaseLocalUnitsTableName)

  AppOptions(options, "lons", required = false, hasArg = true,
    "NAMESPACE", "local-table-namespace", "Local table namespace",
    OptionNames.HBaseLocalUnitsNamespace)

  AppOptions(options, "locf", required = false, hasArg = true,
    "COLUMN FAMILY", "local-column-family", "Local column family",
    OptionNames.HBaseLocalUnitsColumnFamily)

  AppOptions(options, "lofp", required = false, hasArg = true,
    "FILE PATH", "local-file-path", "Local file path",
    OptionNames.HBaseLocalUnitsFilePath)

  AppOptions(options, "retn", required = false, hasArg = true,
    "TABLE NAME", "reporting-table-name", "HBase Reporting table name",
    OptionNames.HBaseReportingUnitsTableName)

  AppOptions(options, "rens", required = false, hasArg = true,
    "NAMESPACE", "reporting-table-namespace", "Reporting table namespace",
    OptionNames.HBaseReportingUnitsNamespace)

  AppOptions(options, "recf", required = false, hasArg = true,
    "COLUMN FAMILY", "reporting-column-family", "Reporting column family",
    OptionNames.HBaseReportingUnitsColumnFamily)

  AppOptions(options, "refp", required = false, hasArg = true,
    "FILE PATH", "reporting-file-path", "Reporting file path",
    OptionNames.HBaseReportingUnitsFilePath)

  AppOptions(options, "tp", required = false, hasArg = true,
    "TIME PERIOD", "time-period", "Time Period",
    OptionNames.TimePeriod)

  AppOptions(options, "paye", required = false, hasArg = true,
    "FILE PATH", "paye-file-path", "PAYE file path",
    OptionNames.PayeFilePath)

  AppOptions(options, "vat", required = false, hasArg = true,
    "FILE PATH", "vat-file-path", "VAT file path",
    OptionNames.VatFilePath)

  AppOptions(options, "geo", required = false, hasArg = true,
    "FILE PATH", "path-to-geo", "GEO file path",
    OptionNames.PathToGeo)

  AppOptions(options, "geoShort", required = false, hasArg = true,
    "FILE PATH", "path-to-geo-short", "GEO short file path",
    OptionNames.PathToGeoShort)

  AppOptions(options, "hiveDB", required = false, hasArg = true,
    "DATABASE NAME", "hive-db-name", "Hive database name",
    OptionNames.HiveDBName)

  AppOptions(options, "hiveTable", required = false, hasArg = true,
    "TABLE NAME", "hive-table-name", "Hive table name",
    OptionNames.HiveTableName)

  AppOptions(options, "hiveShortTable", required = false, hasArg = true,
    "TABLE NAME", "hive-short-table-name", "Hive short table name",
    OptionNames.HiveShortTableName)

  val optionGroup = new OptionGroup
  optionGroup.addOption(version)
  options.addOptionGroup(optionGroup)

  def apply(args: Array[String]): Unit = {

    val parser = new DefaultParser

    try {

      val line: CommandLine = parser.parse(options, args)

      val env = line.getOptionValue(environment.getOpt)
      if (env != "cluster" && env != "local") {
        println("One of cluster or local expected for environment option")
        System.exit(1)
      } else System.setProperty(OptionNames.Environment, line.getOptionValue(environment.getOpt))

      if (line.hasOption(help.getOpt)) {
        val formatter = new HelpFormatter
        formatter.setWidth(100)
        val header = "\n(Re)Run the SBR Enterprise Assembler\n\n"

        formatter.printHelp(s"Enterprise Assembler", header, options, null, true)
        System.exit(0)
      }

      if (line.hasOption(version.getOpt)) {
        println(s"Enterprise Assembler, v1.0")
        System.exit(0)
      }

      if (line.hasOption(debug.getOpt)) System.setProperty("app.debug", "true")

      // Other Options
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
      System.setProperty(v.optionName, name.getValue)
    }
  }
}

class AppOptions(shortOpt: String, required: Boolean, hasArg: Boolean, argName: String,
                 longOpt: String, desc: String, val optionName: String)(implicit val options: Options) {

  val option: Option = Option.builder(shortOpt)
    .required(required)
    .hasArg(hasArg)
    .argName(argName)
    .longOpt(longOpt)
    .desc(desc)
    .build

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
