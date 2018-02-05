package global


import com.typesafe.config._


object ApplicationContext {
  val config = ConfigFactory.load()

}
