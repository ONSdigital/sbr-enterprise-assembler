package model.domain

import java.util

import scala.util.Try

/**
  *
  */
case class Enterprise(ern:String, idbrref:Option[String], businessName:Option[String], PostCode:Option[String],
                      sic07:Option[String],legalStatus:Option[String], payeEmployees:Option[String],
                      payeJobs:Option[String],appTurnover:Option[String], entTurnover:Option[String],
                      cntdTurnover:Option[String], stdTurnover:Option[String],grp_turnover:Option[String]

                     )

object Enterprise{

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key:String) = Try{new String(row.get(key.getBytes))}.toOption

    new Enterprise(
      new String(row.get("ern".getBytes)),
      getValue("entref"),
      getValue("name"),
      getValue("postcode"),
      getValue("legal_status"),
      getValue("sic07"),
      getValue("paye_empees"),
      getValue("paye_jobs"),
      getValue("app_turnover"),
      getValue("ent_turnover"),
      getValue("cntd_turnover"),
      getValue("std_turnover"),
      getValue("grp_turnover")
    )
  }

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)


  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {

    def getValue(qualifier:String) = {
      Try{entry._2.find(_._1==qualifier).get._2}.toOption match{
        case opt@Some(str) if (str.trim().nonEmpty) => opt
        case _ => None
      }

    }
    val ern = entry._2.find(_._1=="ern").get._2

    new Enterprise(
      ern,
      getValue("entref"),
      getValue("name"),
      getValue("postcode"),
      getValue("legal_status"),
      getValue("sic07"),
      getValue("paye_empees"),
      getValue("paye_jobs"),
      getValue("app_turnover"),
      getValue("ent_turnover"),
      getValue("cntd_turnover"),
      getValue("std_turnover"),
      getValue("grp_turnover")
    )
  }


}