package closures.mocks

import closures.BaseClosure
import org.apache.spark.sql.Row
import spark.extensions.sql._

trait MockUnitsIdGenerator {
  this: BaseClosure =>

  val ernMapping: Map[String, String]
  val lurnMapping: Map[String, String]
  val rurnMapping: Map[String, String]
  val prnMapping: Map[String, String]

  override def generateErn(row: Row): String =
    ernMapping(row.getString("name").get)

  override def generateLurn(row: Row): String = {
    val key = row.getString("name").get
    lurnMapping(key)
  }

  override def generateRurn(row: Row): String = {
    val key = row.getString("name").get
    rurnMapping(key)
  }

  override def generatePrn(row: Row): String = {
    val key = row.getString("name").get
    prnMapping(key)
  }

  override def generateLurnFromEnt(row: Row): String =
    lurnMapping(generateLurn(row))
}
