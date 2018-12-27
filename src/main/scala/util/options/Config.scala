package util.options

import com.typesafe.config.{ConfigException, ConfigFactory}

object Config extends Serializable {

  private var config = ConfigFactory.load()

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

  /**
    * if (HBaseZookeeperQuorum != null) {
    *     hbaseConfiguration.set(OptionNames.HBaseZookeeperQuorum, HBaseZookeeperQuorum)
    * }
    *
    * if (HBaseZookeeperPort != null) {
    *     hbaseConfiguration.set(OptionNames.HBaseZookeeperClientPort, HBaseZookeeperPort)
    * }
    *
    *   hbaseConfiguration.setInt(OptionNames.HFilesPerRegion, 500)
    *   hbaseConfiguration.setInt("hbase.rpc.timeout", 360000)
    *   hbaseConfiguration.setInt("hbase.client.scanner.timeout.period", 360000)
    *   hbaseConfiguration.setInt("hbase.cells.scanned.per.heartbeat.check", 60000)
    *
    * @param key
    * @param value
    */
  def add(key: String, value: String): Unit = {

    System.setProperty(key, value)
    config = ConfigFactory.load()
    ConfigFactory.invalidateCaches()
  }

  def set(key: String, value: String): Unit = add(key, value)

  def remove(key: String): Unit = System.setProperty(key, null)

  def apply(path: String): String = get(path)

  def getInt(path: String): Int = config.getInt(path)

  def exists(path: String): Boolean = get(path) != NOT_FOUND

}
