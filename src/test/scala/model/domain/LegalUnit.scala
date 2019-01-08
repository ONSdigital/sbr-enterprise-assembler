package model.domain

import scala.util.Try

/**
  *
  */
case class LegalUnit(
                      ubrn: String,
                      ern: String,
                      prn: String,
                      crn: Option[String],
                      name: String,
                      trading_style: Option[String],
                      address1: String,
                      address2: Option[String],
                      address3: Option[String],
                      address4: Option[String],
                      address5: Option[String],
                      postcode: String,
                      sic07: String,
                      paye_jobs: Option[String],
                      turnover: Option[String],
                      legal_status: String,
                      trading_status: Option[String],
                      birth_date: String,
                      death_date: Option[String],
                      death_code: Option[String],
                      uprn: Option[String]
                    )

object LegalUnit {

  def getValue(entry: (String, Iterable[(String, String)]), qualifier: String): Option[String] = Try {
    entry._2.find(_._1 == qualifier).get._2
  }.toOption match {
    case opt@Some(str) if str.trim().nonEmpty => opt
    case _ => None
  }

  implicit def buildFromHFileDataMap(entry: (String, Iterable[(String, String)])): LegalUnit = {

    val ubrn = entry._2.find(_._1 == "ubrn").get._2
    val ern = entry._1.split("~").head.reverse
    val prn = entry._2.find(_._1 == "prn").get._2
    val name = entry._2.find(_._1 == "name").map(_._2).getOrElse("")
    val address1 = entry._2.find(_._1 == "address1").get._2
    val postcode = entry._2.find(_._1 == "postcode").get._2
    val sic07 = entry._2.find(_._1 == "sic07").get._2
    val legalStatus = entry._2.find(_._1 == "legal_status").get._2
    val dob = entry._2.find(_._1 == "birth_date").get._2

    new LegalUnit(
      ubrn,
      ern,
      prn,
      getValue(entry, "crn"),
      name,
      getValue(entry, "trading_style"),
      address1,
      getValue(entry, "address2"),
      getValue(entry, "address3"),
      getValue(entry, "address4"),
      getValue(entry, "address5"),
      postcode,
      sic07,
      getValue(entry, "paye_jobs"),
      getValue(entry, "turnover"),
      legalStatus,
      getValue(entry, "trading_status"),
      dob,
      getValue(entry, "death_date"),
      getValue(entry, "death_code"),
      getValue(entry, "uprn")
    )

  }
}
