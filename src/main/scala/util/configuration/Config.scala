package util.configuration

import com.typesafe.config.{ConfigException, ConfigFactory}

object Config extends Serializable {

  private val config = ConfigFactory.load()

  lazy val NOT_FOUND = "NOT_FOUND"
  lazy val CONFIG_ERROR = "CONFIG_ERROR"

  def apply(path: String): String = {
    try {
      config.getString(path)
    } catch {
      case ioe: ConfigException.Missing => path + ":" + NOT_FOUND
      case e: Exception => CONFIG_ERROR
    }
  }
}
