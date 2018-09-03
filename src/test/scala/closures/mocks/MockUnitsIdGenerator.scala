package closures.mocks

import closures.BaseClosure
import global.AppParams
import org.apache.spark.sql.Row
import spark.extensions.sql._

trait MockUnitsIdGenerator {this:BaseClosure =>

  val ernMapping: Map[String, String]
  val lurnMapping: Map[String, String]
  val rurnMapping: Map[String, String]
  val prnMapping: Map[String, String]


  override def generateErn(row: Row, appParams: AppParams) = ernMapping(row.getString("BusinessName").get)

  override def generateLurn(row: Row, appParams: AppParams) = {
    val key  = Seq(row.getString("BusinessName"),row.getString("name")).collect{case Some(name) => name}.head
    lurnMapping(key)
  }
  override def generateRurn(row: Row, appParams: AppParams) = {
    val key  = Seq(row.getString("BusinessName"),row.getString("name")).collect{case Some(name) => name}.head
    rurnMapping(key)
  }

  override def generatePrn(row: Row, appParams: AppParams) = {
    val key  = Seq(row.getString("BusinessName"),row.getString("name")).collect{case Some(name) => name}.head
    prnMapping(key)
  }

  override def generateLurnFromEnt(row: Row, appParams: AppParams) = lurnMapping(generateLurn(row, appParams))
}
