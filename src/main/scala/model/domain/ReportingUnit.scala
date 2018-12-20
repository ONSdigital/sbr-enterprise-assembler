package model.domain

import scala.util.Try

/**
  *
  */
case class ReportingUnit(
                          rurn:String,
                          ruref:Option[String],
                          ern:String,
                          entref:Option[String],
                          prn:String,
                          name:String,
                          tradingStyle:Option[String],
                          legalStatus:String,
                          address1:String,
                          address2:Option[String],
                          address3:Option[String],
                          address4:Option[String],
                          address5:Option[String],
                          postcode:String,
                          sic07:String,
                          region:String,
                          employees:String,
                          employment:String,
                          turnover:String

                      )

object ReportingUnit {

      def getValue(entry:(String, Iterable[(String, String)]),qualifier:String): Option[String] = Try{entry._2.find(_._1==qualifier).get._2}.toOption match {
        case opt@Some(str) if str.trim().nonEmpty => opt
        case _ => None
      }

      implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])): ReportingUnit = {

          val rurn = entry._2.find (_._1 == "rurn").get._2
          val ern = entry._2.find (_._1 == "ern").get._2
          val name = entry._2.find (_._1 == "name").map (_._2).getOrElse ("")
          val address1 = entry._2.find (_._1 == "address1").get._2
          val postcode = entry._2.find (_._1 == "postcode").get._2
          val sic07 = entry._2.find (_._1 == "sic07").get._2
          val legalStatus = entry._2.find (_._1 == "legal_status").get._2
          val employees = entry._2.find (_._1 == "employees").get._2
          val region = entry._2.find (_._1 == "region").get._2
          val employment = entry._2.find (_._1 == "employment").get._2
          val turnover = entry._2.find (_._1 == "turnover").get._2
          val prn = entry._2.find (_._1 == "prn").get._2

          new ReportingUnit(
            rurn,
            getValue(entry,"ruref"),
            ern,
            getValue(entry,"entref"),prn,name,getValue(entry,"trading_style"),legalStatus,address1,
            getValue(entry,"address2"),getValue(entry,"address3"),getValue(entry,"address4"),getValue(entry,"address5"),
            postcode,sic07,region,employees, employment,turnover
          )
    }
}