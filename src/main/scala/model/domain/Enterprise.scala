package model.domain

import scala.util.Try

/**
  *
  */
case class Enterprise(
                       ern:String,
                       prn:String,
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
                       grp_turnover:Option[String],
                       workingProps:String,
                       employment:String,
                       region:String
                     )

object Enterprise{

  def apply(row:HFileRow): Enterprise = {

    val cells = row.cells
    val ern = cells.find(_.column == "ern")
    val prn = cells.find(_.column == "prn")
    val entref = cells.find(_.column == "entref")
    val name= cells.find(_.column == "name")
    val address1 = cells.find(_.column == "address1")
    val address2 = cells.find(_.column == "address2")
    val address3 = cells.find(_.column == "address3")
    val address4 = cells.find(_.column == "address4")
    val address5 = cells.find(_.column == "address5")
    val postcode= cells.find(_.column == "postcode")
    val legalStatus= cells.find(_.column == "legal_status")
    val tradingStyle= cells.find(_.column == "trading_style")
    val sic07= cells.find(_.column == "sic07")
    val empees= cells.find(_.column == "paye_empees")
    val jobs= cells.find(_.column == "paye_jobs")
    val appTurnover= cells.find(_.column == "app_turnover")
    val entTurnover= cells.find(_.column == "ent_turnover")
    val cntdTurnover= cells.find(_.column == "cntd_turnover")
    val stdTurnover= cells.find(_.column == "std_turnover")
    val grpTurnover= cells.find(_.column == "grp_turnover")
    val working_props= cells.find(_.column == "working_props")
    val employment= cells.find(_.column == "employment")
    val region = cells.find(_.column == "region")

    new Enterprise(
                    ern.get.value,
                    prn.get.value,
                    entref.map(_.value),
                    name.get.value,
                    address1.get.value,
                    address2.map(_.value),
                    address3.map(_.value),
                    address4.map(_.value),
                    address5.map(_.value),
                    postcode.get.value,
                    tradingStyle.map(_.value),
                    sic07.get.value,
                    legalStatus.get.value,
                    empees.map(_.value),
                    jobs.map(_.value),
                    appTurnover.map(_.value),
                    entTurnover.map(_.value),
                    cntdTurnover.map(_.value),
                    stdTurnover.map(_.value),
                    grpTurnover.map(_.value),
                    working_props.get.value,
                    employment.get.value,
                    region.get.value
                  )
  }

  def apply(entry:(String, Iterable[(String, String)])): Enterprise = buildFromHFileDataMap(entry)


  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])): Enterprise = {

    def getValue(qualifier:String) = {
      Try{entry._2.find(_._1==qualifier).get._2}.toOption match{
        case opt@Some(str) if (str.trim().nonEmpty) => opt
        case _ => None
      }
    }
    val ern = entry._2.find(_._1=="ern").get._2
    val prn = entry._2.find(_._1=="prn").get._2
    val working_props = entry._2.find(_._1 == "working_props").get._2
    val employment = entry._2.find(_._1 == "employment").get._2
    val region = entry._2.find(_._1 == "region").get._2

    new Enterprise(
      ern,
      prn,
      getValue("entref"),
      getValue("name").get,
      getValue("address1").getOrElse(""),
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
      getValue("grp_turnover"),
      working_props,
      employment,
      region
    )
  }

}