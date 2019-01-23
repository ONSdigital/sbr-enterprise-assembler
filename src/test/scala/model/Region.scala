package model


import scala.language.implicitConversions
import scala.util.Try

/**
  *
  */
case class Region(
                      ern: String,
                      region: String
                    )

object Region {

  def apply(entry: (String, Iterable[(String, String)])): Region = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry: (String, Iterable[(String, String)])): Region = {

    def getValue(qualifier: String) = {
      Try {
        entry._2.find(_._1 == qualifier).get._2
      }.toOption match {
        case opt@Some(str) if (str.trim().nonEmpty) => opt
        case _ => None
      }
    }

    val ern = entry._2.find(_._1 == "ern").get._2
    val region = entry._2.find(_._1 == "region").get._2


    new Region(
      ern,
      region
    )
  }
}

