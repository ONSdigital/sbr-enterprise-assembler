package model

import scala.language.implicitConversions
import scala.util.Try

/**
  *
  */
case class Employment(
                   ern: String,
                   employment: String
                 )

object Employment {

  def apply(entry: (String, Iterable[(String, String)])): Employment = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry: (String, Iterable[(String, String)])): Employment = {

    def getValue(qualifier: String) = {
      Try {
        entry._2.find(_._1 == qualifier).get._2
      }.toOption match {
        case opt@Some(str) if (str.trim().nonEmpty) => opt
        case _ => None
      }
    }

    val ern = entry._2.find(_._1 == "ern").get._2
    val employment = entry._2.find(_._1 == "employment").get._2


    new Employment(
      ern,
      employment
    )
  }
}

