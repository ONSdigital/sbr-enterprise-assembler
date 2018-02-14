package global


import com.typesafe.config._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.LoggerFactory

import scala.util.Try


object Configured {

  val logger = LoggerFactory.getLogger(getClass)

  val config: Config = ConfigFactory.load()


  val conf: Configuration = HBaseConfiguration.create()
  Try{config.getString("hbase.kerberos.config")}.map(conf.addResource).getOrElse(logger.info("no config resource for kerberos specified"))
  Try{config.getString("hbase.path.config")}.map(conf.addResource).getOrElse {
    logger.info("no config resource for hbase specified. Default configs will be used")
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeper.url"))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", config.getInt("hbase.files.per.region"))
  }
  Try{config.getString("hbase.table.name")}.map(conf.set("hbase.table.name",_)).getOrElse(conf.set("hbase.table.name","enterprise"))
  Try{config.getString("table.column.family")}.map(conf.set("table.column.family",_)).getOrElse(conf.set("table.column.family","l"))

  Try{config.getString("files.json")}.map(conf.set("files.json",_)).getOrElse(conf.set("files.json","src/main/resources/data/sample.json"))
  Try{config.getString("files.parquet")}.map(conf.set("files.parquet",_)).getOrElse(conf.set("files.parquet","src/main/resources/data/sample.parquet"))
  Try{config.getString("files.hfile")}.map(conf.set("files.hfile",_)).getOrElse(conf.set("files.hfile","src/main/resources/data/hfile"))

   lazy val PATH_TO_JSON = conf.getStrings("files.json").head
   lazy val PATH_TO_PARQUET = conf.getStrings("files.parquet").head
   lazy val PATH_TO_HFILE =  conf.getStrings("files.hfile").head
   lazy val HBASE_ENTERPRISE_TABLE_NAME = conf.getStrings("hbase.table.name").head
   lazy val HBASE_ENTERPRISE_TABLE_NAMESPACE = conf.getStrings("hbase.table.namespace").head
   lazy val HBASE_ENTERPRISE_COLUMN_FAMILY = conf.getStrings("table.column.family").head




}
