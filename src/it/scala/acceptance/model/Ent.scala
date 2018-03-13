package acceptance.model

import java.util

import scala.util.Try

/**
  *
  */
case class Ent(ern:String,idbrref:Option[String],businessName:Option[String],PostCode:Option[String], LegalStatus:Option[String])

object Ent{

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key:String) = Try{new String(row.get(key.getBytes))}.toOption


    new Ent(
      new String(row.get("ern".getBytes)),
      getValue("idbrref"),
      getValue("name"),
      getValue("postcode"),
      getValue("legalstatus"),
      getValue("avg"),
      getValue("sum(dec_jobs)"))
  }
}