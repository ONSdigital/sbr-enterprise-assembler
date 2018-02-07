package global


import com.typesafe.config._

import scala.util.Try


object ApplicationContext {

 val config: Config = ConfigFactory.load()


   val PATH_TO_JSON = config.getString("files.json")
   val PATH_TO_PARQUET = config.getString("files.parquet")
   val PATH_TO_HFILE = config.getString("files.hfile")

/*   val PERIOD = config.getString("files.hfile")
   val ID_KEY = config.getString("files.hfile")*/



}
