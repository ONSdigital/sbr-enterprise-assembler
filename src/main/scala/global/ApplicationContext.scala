package global


import com.typesafe.config._
import connector.HBaseConnector.getClass
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.LoggerFactory

import scala.util.Try


object ApplicationContext {

  val logger = LoggerFactory.getLogger(getClass)

 val config: Config = ConfigFactory.load()


  val conf: Configuration = HBaseConfiguration.create()
  Try{config.getString("hbase.kerberos.config")}.map(conf.addResource).getOrElse(logger.info("no config resource for kerberos specified"))
  Try{config.getString("hbase.local.path.config")}.map(conf.addResource).getOrElse {
    logger.info("no config resource for hbase specified. Default configs will be used")
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.local.zookeper.url"))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", config.getInt("hbase.local.files.per.region"))
  }

   val PATH_TO_JSON = config.getString("files.json")
   val PATH_TO_PARQUET = config.getString("files.parquet")
   val PATH_TO_HFILE = config.getString("files.hfile")




}
