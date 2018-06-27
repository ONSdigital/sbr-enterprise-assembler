package model.domain

import java.util

import scala.util.Try

/**
  *
  */
case class Enterprise(ern:String,
                      idbrref:Option[String],
                      businessName:String,
                      address1:String,
                      address2:Option[String],
                      address3:Option[String],
                      address4:Option[String],
                      address5:Option[String],
                      PostCode:String,
                      tradingStyle:Option[String],
                      sic07:String,
                      legalStatus:String,
                      payeEmployees:Option[String],
                      payeJobs:Option[String],
                      appTurnover:Option[String],
                      entTurnover:Option[String],
                      cntdTurnover:Option[String],
                      stdTurnover:Option[String],
                      grp_turnover:Option[String]

                     )

object Enterprise{

/*  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

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
  }*/

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)


  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {

    def getValue(qualifier:String) = {
      Try{entry._2.find(_._1==qualifier).get._2}.toOption match{
        case opt@Some(str) if (str.trim().nonEmpty) => opt
        case _ => None
      }

    }
    val ern = entry._2.find(_._1=="ern").get._2
//ern	entref	name	trading_style	address1	address2	address3	address4	address5	postcode	legal_status	sic07
    //paye_empees
    //paye_jobs
    new Enterprise(
      ern,
      getValue("entref"),
      getValue("name").get,
      getValue("address1").get,
      getValue("address2"),
      getValue("address3"),
      getValue("address4"),
      getValue("address5"),
      getValue("postcode").get,
      getValue("trading_style"),
      getValue("legal_status").get,
      getValue("sic07").get,
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