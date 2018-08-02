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

  def apply(row:HFileRow) = {

    val cells = row.cells
    val ern = cells.find(_.column == "ern")
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

    new Enterprise(
        ern.get.value,
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
        grpTurnover.map(_.value)
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
//ern	entref	name	trading_style	address1	address2	address3	address4	address5	postcode	legal_status	sic07
    //paye_empees
    //paye_jobs
    new Enterprise(
      ern,
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
      getValue("grp_turnover")
    )
  }


}