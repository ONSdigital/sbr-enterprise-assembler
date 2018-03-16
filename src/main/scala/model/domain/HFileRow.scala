package model.domain

/**
  *
  */


case class HFileRow(key:String, cells:Seq[HFileCell[String,String]])
object HFileRow{

  def apply(entry:(String, Iterable[(String, String)])) = new HFileRow(entry._1, entry._2.map(c => HFileCell(c)).toSeq)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = HFileRow(entry)
}