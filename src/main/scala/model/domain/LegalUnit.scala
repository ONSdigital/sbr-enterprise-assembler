package model.domain

/**
  *
  */
case class PayeData(payeRef:String,q1:Option[String],q2:Option[String],q3:Option[String],q4:Option[String])
object PayeData{
  def apply(payeRef:String) = new PayeData(payeRef, None,None,None,None)
}

case class VatData(vatRef:String,turnover:Option[String],recordType:Option[String])
object VatData{
  def apply(vatRef:String) = new VatData(vatRef,None,None)
}



case class LegalUnitLink(ubrn:String, ch:Option[String], payeRefs:Seq[String], varRefs:Seq[String])

case class LinkRecord(ern:String, lurns:Seq[String], leus:Seq[LegalUnitLink])

object LinkRecord{

  //def getId(rows:Seq[String], unitCode:String) = rows.collect{case HFileRow(key,cells) if(key.contains(s"~$unitCode~")) => key.split("~").head}

  def getLinkRecords(rows:Seq[HFileRow]) = {
    val erns: Seq[String] = rows.collect{case HFileRow(key,cells) if(key.contains("~ENT~")) => key.split("~").head}
    erns.map(ern => {
      val ubrns = rows.collect{case HFileRow(key,cells) if(key.contains("~LEU~") && cells.find(_.value==ern).isDefined) => key.split("~").head}.sortBy(identity(_))
      val leus = ubrns.map(ubrn => {
        val ch = rows.collect{case HFileRow(key,cells) if(key.contains("~CH~") && cells.find(cell => cell.value==ubrn && cell.column=="p_LEU").isDefined) => key.split("~").head}.headOption
        val payeRefs = rows.collect{case HFileRow(key,cells) if(key.contains("~PAYE~") && cells.find(cell => cell.value==ubrn && cell.column=="p_LEU").isDefined) => key.split("~").head}.sortBy(identity(_))
        val vatRefs = rows.collect{case HFileRow(key,cells) if(key.contains("~VAT~") && cells.find(cell => cell.value==ubrn && cell.column=="p_LEU").isDefined) => key.split("~").head}.sortBy(identity(_))
        LegalUnitLink(ubrn,ch,payeRefs, vatRefs)
      }).sortBy(_.ubrn)

      val lurns = rows.collect{case HFileRow(key,cells) if(key.contains(s"$ern~ENT~")) => cells.collect{case cell if (cell.value=="LOU") => cell.column.replace("c_","")}}.flatten.sortBy(identity(_))
      new LinkRecord(ern, lurns, leus)}
    ).sortBy(_.ern)
  }
}