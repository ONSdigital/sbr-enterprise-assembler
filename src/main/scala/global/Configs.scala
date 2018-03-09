package global


import com.typesafe.config._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.LoggerFactory

import scala.util.Try


object Configs{

  val logger = LoggerFactory.getLogger(getClass)

  val config: Config = ConfigFactory.load()

  val conf: Configuration = HBaseConfiguration.create()
  Try{config.getString("hadoop.security.authentication")}.map(conf.addResource).getOrElse(conf.set("hadoop.security.authentication","kerberos"))
  Try{config.getString("hbase.security.authentication")}.map(conf.addResource).getOrElse(conf.set("hbase.security.authentication","kerberos"))
  Try{config.getString("hbase.kerberos.config")}.map(conf.addResource).getOrElse(logger.info("no config resource for kerberos specified"))
  Try{config.getString("hbase.path.config")}.map(conf.addResource).getOrElse {
    logger.info("no config resource for hbase specified. Default configs will be used")
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeper.url"))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", config.getInt("hbase.files.per.region"))
  }
  Try{config.getString("hbase.table.links.name")}.map(conf.set("hbase.table.links.name",_)).getOrElse(conf.set("hbase.table.links.name","LINKS"))
  Try{config.getString("hbase.table.links.column.family")}.map(conf.set("hbase.table.links.column.family",_)).getOrElse(conf.set("hbase.table.links.column.family","l"))
  Try{config.getString("hbase.table.links.namespace")}.map(conf.set("hbase.table.links.namespace",_)).getOrElse(conf.set("hbase.table.links.namespace","ons"))

  Try{config.getString("hbase.table.enterprise.name")}.map(conf.set("hbase.table.enterprise.name",_)).getOrElse(conf.set("hbase.table.enterprise.name","ENT"))
  Try{config.getString("hbase.table.enterprise.column.family")}.map(conf.set("hbase.table.enterprise.column.family",_)).getOrElse(conf.set("hbase.table.enterprise.column.family","d"))
  Try{config.getString("hbase.table.enterprise.namespace")}.map(conf.set("hbase.table.enterprise.namespace",_)).getOrElse(conf.set("hbase.table.enterprise.namespace","ons"))

  Try{config.getString("files.json")}.map(conf.set("files.json",_)).getOrElse(conf.set("files.json","src/main/resources/data/sample.json"))
  Try{config.getString("files.parquet")}.map(conf.set("files.parquet",_)).getOrElse(conf.set("files.parquet","src/main/resources/data/sample.parquet"))
  Try{config.getString("files.links.hfile")}.map(conf.set("files.links.hfile",_)).getOrElse(conf.set("files.hfile","src/main/resources/data/links/hfile"))
  Try{config.getString("files.enterprise.hfile")}.map(conf.set("files.enterprise.hfile",_)).getOrElse(conf.set("files.hfile","src/main/resources/data/enterprise/hfile"))

   lazy val PATH_TO_JSON = conf.getStrings("files.json").head
   lazy val PATH_TO_PARQUET = conf.getStrings("files.parquet").head

   lazy val PATH_TO_LINKS_HFILE =  conf.getStrings("files.links.hfile").head
   lazy val PATH_TO_ENTERPRISE_HFILE =  conf.getStrings("files.enterprise.hfile").head

   lazy val HBASE_LINKS_TABLE_NAME = conf.getStrings("hbase.table.links.name").head
   lazy val HBASE_LINKS_TABLE_NAMESPACE = conf.getStrings("hbase.table.links.namespace").head
   lazy val HBASE_LINKS_COLUMN_FAMILY = conf.getStrings("hbase.table.links.column.family").head

   lazy val HBASE_ENTERPRISE_TABLE_NAME = conf.getStrings("hbase.table.enterprise.name").head
   lazy val HBASE_ENTERPRISE_TABLE_NAMESPACE = conf.getStrings("hbase.table.enterprise.namespace").head
   lazy val HBASE_ENTERPRISE_COLUMN_FAMILY = conf.getStrings("hbase.table.enterprise.column.family").head


  def updateConf(args: Array[String]) = {

    Try(args(0)).map(conf.set("hbase.table.links.name", _)).getOrElse(Unit)
    Try(args(1)).map(conf.set("hbase.table.links.namespace", _)).getOrElse(Unit)
    Try(args(2)).map(conf.set("files.links.hfile", _)).getOrElse(Unit)

    Try(args(3)).map(conf.set("hbase.table.enterprise.name", _)).getOrElse(Unit)
    Try(args(4)).map(conf.set("hbase.table.enterprise.namespace", _)).getOrElse(Unit)
    Try(args(5)).map(conf.set("files.enterprise.hfile", _)).getOrElse(Unit)

    Try(args(6)).map(conf.set("files.parquet", _)).getOrElse(Unit)
    Try(args(7)).map(conf.set("hbase.zookeeper.quorum", _)).getOrElse(Unit)
    Try(args(8)).map(conf.set("hbase.zookeeper.property.clientPort", _)).getOrElse(Unit)
    //Try(args(9)).map(conf.set("enterprise.data.timeperiod", _)).getOrElse(Unit)

  }

}
