package model.domain

/**
  *
  */
case class LinkRow(cells:Seq[HFileCell[String,String]])

object LinkRow {

  def apply(row:HFileRow) = new LinkRow(row.cells)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = LinkRow(entry._2.map(HFileCell(_)).toSeq)

}
