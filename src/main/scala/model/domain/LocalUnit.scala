package model.domain

import java.util

import scala.util.Try

case class LocalUnit(lurn:String,	luref:Option[String],	ern:String,	entref:Option[String],	name:String,	tradingstyle:Option[String],
                     address1:String,	address2:Option[String],	address3:Option[String],address4:Option[String],address5:Option[String],
                     postcode:String,sic07:String,employees:String) {

}

object LocalUnit {

  def getValue(entry:(String, Iterable[(String, String)]),qualifier:String) = Try{entry._2.find(_._1==qualifier).get._2}.toOption match{
    case opt@Some(str) if (str.trim().nonEmpty) => opt
    case _ => None
  }

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {


    val ern = entry._2.find(_._1=="ern").get._2
    val lurn = entry._2.find(_._1=="lurn").get._2
    val name = entry._2.find(_._1=="name").get._2
    val address1 = entry._2.find(_._1=="address1").get._2
    val postcode = entry._2.find(_._1=="postcode").get._2
    val sic07 = entry._2.find(_._1=="sic07").get._2
    val employees = entry._2.find(_._1=="employees").get._2

    new LocalUnit(
      lurn, getValue(entry,"luref"),ern, getValue(entry,"entref"), name,getValue(entry,"tradingstyle"), address1,
      getValue(entry,"address2"),getValue(entry,"address3"),getValue(entry,"address4"),getValue(entry,"address5"),
      postcode,sic07,employees
    )

  }
}
