package util

import com.typesafe.config.{ConfigException, ConfigFactory}

object Config extends Serializable {

  private val config =  ConfigFactory.load()

  lazy val NOT_FOUND = "NOT_FOUND"
  lazy val CONFIG_ERROR = "CONFIG_ERROR"

  private def get(path: String): String = {
    try {
      config.getString(path)
    } catch {
      case ioe: ConfigException.Missing => NOT_FOUND
      case e: Exception => CONFIG_ERROR
    }
  }

  def add(key: String, value: String): Unit = System.setProperty(key, value)
  def remove(key: String): Unit = System.setProperty(key, null)

  def apply(path: String): String = get(path)

  def getInt(path: String): Int = config.getInt(path)

  def exists(path: String): Boolean =  get(path) != NOT_FOUND

}
