package global


import com.typesafe.config._

import scala.util.Try


object ApplicationContext {
  lazy val config: Config = ConfigFactory.load()



   def getValue(key:String) = Try{config.getString("files.env.config")}
                                 .map(ConfigFactory.load(_).getString(key)).getOrElse(config.getString(key))

   val PATH_TO_JSON = getValue("files.json")
   val PATH_TO_PARQUET = getValue("files.parquet")
   val PATH_TO_HFILE = getValue("files.hfile")



}
