package closures.mocks

import closures.CreateClosures
import dao.hbase.HFileUtils
import dao.hbase.converter.WithConversionHelper
import global.AppParams
import model.hfile.Tables
import org.apache.spark.sql.Row
import spark.extensions.sql.SqlRowExtensions


trait MockClosures{this:HFileUtils =>

  val ernMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "111111111-TEST-ERN"),
    ("ACCLAIMED HOMES LIMITED" ->       "222222222-TEST-ERN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "333333333-TEST-ERN")
  )

  val lurnMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "11111111-TEST-LURN"),
    ("ACCLAIMED HOMES LIMITED" ->       "22222222-TEST-LURN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "33333333-TEST-LURN")
  )

  override def generateErn(row: Row, appParams: AppParams) = ernMapping(row.getString("BusinessName").get)

  override def generateLurn(row: Row, appParams: AppParams) = lurnMapping(row.getString("BusinessName").get)

  override def generateLurnFromEnt(row: Row, appParams: AppParams) = lurnMapping(row.getString("name").get)




}

