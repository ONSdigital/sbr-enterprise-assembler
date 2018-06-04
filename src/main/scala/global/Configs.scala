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
  conf.setInt("hbase.rpc.timeout", 300000)
  conf.setInt("hbase.client.scanner.timeout.period", 300000)
  //conf.setInt("hbase.cells.scanned.per.heartbeat.check", 50000)
  Try{config.getString("hadoop.security.authentication")}.map(conf.addResource).getOrElse(conf.set("hadoop.security.authentication","kerberos"))
  Try{config.getString("hbase.security.authentication")}.map(conf.addResource).getOrElse(conf.set("hbase.security.authentication","kerberos"))
  Try{config.getString("hbase.kerberos.config")}.map(conf.addResource).getOrElse(logger.info("no config resource for kerberos specified"))
  Try{config.getString("hbase.path.config")}.map(conf.addResource).getOrElse {
    logger.info("no config resource for hbase specified. Default configs will be used")
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeper.url"))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 500)
  }
  lazy val PATH_TO_JSON = config.getString("files.json")

}
