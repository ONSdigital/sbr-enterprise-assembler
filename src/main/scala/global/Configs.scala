package global


import com.typesafe.config._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.LoggerFactory

import scala.util.Try


object Configs{

  val logger = LoggerFactory.getLogger(getClass)

  val  defaultTimePeriod = "timeperiod-not-specified"

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
  lazy val PATH_TO_JSON = conf.getStrings("files.json").head
 /* Try{config.getString("hbase.table.links.name")}.map(conf.set("hbase.table.links.name",_)).getOrElse(conf.set("hbase.table.links.name","LINKS"))
  Try{config.getString("hbase.table.links.column.family")}.map(conf.set("hbase.table.links.column.family",_)).getOrElse(conf.set("hbase.table.links.column.family","l"))
  Try{config.getString("hbase.table.links.namespace")}.map(conf.set("hbase.table.links.namespace",_)).getOrElse(conf.set("hbase.table.links.namespace","ons"))

  Try{config.getString("hbase.table.enterprise.name")}.map(conf.set("hbase.table.enterprise.name",_)).getOrElse(conf.set("hbase.table.enterprise.name","ENT"))
  Try{config.getString("hbase.table.enterprise.column.family")}.map(conf.set("hbase.table.enterprise.column.family",_)).getOrElse(conf.set("hbase.table.enterprise.column.family","d"))
  Try{config.getString("hbase.table.enterprise.namespace")}.map(conf.set("hbase.table.enterprise.namespace",_)).getOrElse(conf.set("hbase.table.enterprise.namespace","ons"))

  Try{config.getString("files.json")}.map(conf.set("files.json",_)).getOrElse(conf.set("files.json","src/main/resources/data/sample.json"))
  Try{config.getString("files.parquet")}.map(conf.set("files.parquet",_)).getOrElse(conf.set("files.parquet","src/main/resources/data/sample.parquet"))
  Try{config.getString("files.links.hfile")}.map(conf.set("files.links.hfile",_)).getOrElse(conf.set("files.hfile","src/main/resources/data/links/hfile"))
  Try{config.getString("files.enterprise.hfile")}.map(conf.set("files.enterprise.hfile",_)).getOrElse(conf.set("files.hfile","src/main/resources/data/enterprise/hfile"))
  Try{config.getString("files.paye.csv")}.map(conf.set("files.paye.csv",_)).getOrElse(conf.set("files.paye.csv","src/main/resources/data/smallPaye.csv"))



  Try{config.getString("enterprise.data.default.timeperiod")}.map(conf.set("enterprise.data.timeperiod",_)).getOrElse(conf.set(s"enterprise.data.timeperiod",defaultTimePeriod))




   lazy val PATH_TO_PARQUET = conf.getStrings("files.parquet").head
   lazy val PATH_TO_PAYE = conf.getStrings("files.paye.csv").head

   lazy val PATH_TO_LINKS_HFILE =  conf.getStrings("files.links.hfile").head
   lazy val PATH_TO_ENTERPRISE_HFILE =  conf.getStrings("files.enterprise.hfile").head

   lazy val HBASE_LINKS_TABLE_NAME = conf.getStrings("hbase.table.links.name").head
   lazy val HBASE_LINKS_TABLE_NAMESPACE = conf.getStrings("hbase.table.links.namespace").head
   lazy val HBASE_LINKS_COLUMN_FAMILY = conf.getStrings("hbase.table.links.column.family").head

   lazy val HBASE_ENTERPRISE_TABLE_NAME = conf.getStrings("hbase.table.enterprise.name").head
   lazy val HBASE_ENTERPRISE_TABLE_NAMESPACE = conf.getStrings("hbase.table.enterprise.namespace").head
   lazy val HBASE_ENTERPRISE_COLUMN_FAMILY = conf.getStrings("hbase.table.enterprise.column.family").head
   lazy val TIME_PERIOD = conf.getStrings("enterprise.data.timeperiod").head

  def updateConf(args: Array[String]) = {

    //args sample:  LINKS ons src/main/resources/data/links/hfile ENT ons src/main/resources/data/enterprise/hfile src/main/resources/data/sample.parquet localhost 2181 201802 src/main/resources/data/smallPaye.csv

    val indexedParams = args.zipWithIndex.toSeq

    val params = indexedParams.map(p => (p._2,p._1)).toMap

    conf.set("hbase.table.links.name", params(0))
    conf.set("hbase.table.links.namespace", params(1))
    conf.set("files.links.hfile", params(2))
    conf.set("hbase.table.enterprise.name", params(3))
    conf.set("hbase.table.enterprise.namespace", params(4))
    conf.set("files.enterprise.hfile", params(5))
    conf.set("files.parquet", params(6))
    conf.set("hbase.zookeeper.quorum", params(7))
    conf.set("hbase.zookeeper.property.clientPort", params(8))
    conf.set("enterprise.data.timeperiod",params(9))
    conf.set("files.paye.csv",params(10))

  }*/
}
