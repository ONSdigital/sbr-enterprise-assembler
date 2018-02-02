package config

/**
  *
  */
object Config {
//Files:
  val pathToJson = "src/main/resources/sample.json"
  val pathToParquet = "src/main/resources/sample.parquet"
  val hfilePath = "src/main/resources/hfile"
//HBase:
  val tableName = "enterprise"
  val columnFamily = "d"
  val zookeeperUrl = "localhost:2181"
  val filesPerRegion = 500
}
