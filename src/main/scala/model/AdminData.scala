package model

import scala.language.implicitConversions
import scala.util.Try

/**
  *
  */
case class AdminData(
                       ern: String,
                       payeEmployees: Option[String],
                       payeJobs: Option[String],
                       cntdTurnover: Option[String],
                       appTurnover: Option[String],
                       stdTurnover: Option[String],
                       grp_turnover: Option[String],
                       entTurnover: Option[String]
                     )

object AdminData {

  def apply(entry: (String, Iterable[(String, String)])): AdminData = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry: (String, Iterable[(String, String)])): AdminData = {

    def getValue(qualifier: String) = {
      Try {
        entry._2.find(_._1 == qualifier).get._2
      }.toOption match {
        case opt@Some(str) if (str.trim().nonEmpty) => opt
        case _ => None
      }
    }

    val ern = entry._2.find(_._1 == "ern").get._2
    
    new AdminData(
      ern,
      getValue("paye_empees"),
      getValue("paye_jobs"),
      getValue("cntd_turnover"),
      getValue("app_turnover"),
      getValue("std_turnover"),
      getValue("grp_turnover"),
      getValue("ent_turnover")
    )
  }

}