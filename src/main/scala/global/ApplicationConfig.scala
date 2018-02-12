package global


import com.typesafe.config._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.LoggerFactory

import scala.util.Try


trait ApplicationConfig {

  val logger = LoggerFactory.getLogger(getClass)

 val config: Config = ConfigFactory.load()


  val conf: Configuration = HBaseConfiguration.create()
  Try{config.getString("hbase.kerberos.config")}.map(conf.addResource).getOrElse(logger.info("no config resource for kerberos specified"))
  Try{config.getString("hbase.local.path.config")}.map(conf.addResource).getOrElse {
    logger.info("no config resource for hbase specified. Default configs will be used")
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.local.zookeper.url"))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", config.getInt("hbase.local.files.per.region"))
  }

   val PATH_TO_JSON = Try{config.getString("files.json")}.getOrElse("src/main/resources/data/sample.json")
   val PATH_TO_PARQUET = Try{config.getString("files.parquet")}.getOrElse("src/main/resources/data/sample.parquet")
   val PATH_TO_HFILE =  Try(config.getString("files.hfile")).getOrElse("src/main/resources/data/hfile")
   val HBASE_ENTERPRISE_TABLE_NAME = Try(config.getString("hbase.local.table.name")).getOrElse("enterprise")




}
