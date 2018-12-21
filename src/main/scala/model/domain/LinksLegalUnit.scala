package model.domain

/**
  *
  */
case class PayeData(payeRef: String, q1: Option[String], q2: Option[String], q3: Option[String], q4: Option[String])

object PayeData {
  def apply(payeRef: String) = new PayeData(payeRef, None, None, None, None)
}

case class VatData(vatRef: String, turnover: Option[String], recordType: Option[String])

object VatData {
  def apply(vatRef: String) = new VatData(vatRef, None, None)
}

case class LegalUnitLink(ubrn: String, ch: Option[String], payeRefs: Seq[String], varRefs: Seq[String])

case class ReportingUnitLink(rurn: String, lurns: Seq[String], ern: String)

case class LinkRecord(ern: String, rus: Seq[ReportingUnitLink], leus: Seq[LegalUnitLink])

object LinkRecord {

  def getLinks(rows: Seq[HFileRow]): Seq[LinkRecord] = {
    rows.collect {
      case HFileRow(key, cells) if key.startsWith("ENT~") => getLinkRecord(key.stripPrefix("ENT~"), rows) }.collect { case Some(record) => record }
  }

  def getLeus(ern: String, rows: Seq[HFileRow]): Seq[LegalUnitLink] = {
    rows.collect { case HFileRow(key, cells) if key.startsWith("LEU~") && cells.find(cell => cell.column == "p_ENT" && cell.value == ern).isDefined => {
      val ubrn = key.stripPrefix("LEU~")
      val ch = getLeuCh(ubrn, rows)
      val payeRefs = getPayeRefs(ubrn, rows)
      val vatRefs = getVatRefs(ubrn, rows)
      LegalUnitLink(ubrn, ch, payeRefs, vatRefs)
    }
    }
  }

  def getLeuCh(ubrn: String, rows: Seq[HFileRow]): Option[String] = {
    rows.collect { case HFileRow(key, cells) if (key.contains("CH~") && cells.find(cell => cell.value == ubrn && cell.column == "p_LEU").isDefined) => key.split("~").last }.headOption
  }

  def getPayeRefs(ubrn: String, rows: Seq[HFileRow]): Seq[String] = {
    rows.collect { case HFileRow(key, cells) if (key.contains("PAYE~") && cells.find(cell => cell.value == ubrn && cell.column == "p_LEU").isDefined) => key.split("~").last }.sortBy(identity(_))
  }

  def getVatRefs(ubrn: String, rows: Seq[HFileRow]): Seq[String] = {
    rows.collect { case HFileRow(key, cells) if (key.contains("VAT~") && cells.find(cell => cell.value == ubrn && cell.column == "p_LEU").isDefined) => key.split("~").last }.sortBy(identity(_))
  }

  def getReus(ern: String, rows: Seq[HFileRow]): Seq[ReportingUnitLink] = rows.collect {
    case HFileRow(key, cells) if (key.startsWith("REU~") && cells.find(cell => cell.column == "p_ENT" && cell.value == ern).isDefined) => getSingleReu(key.stripPrefix("REU~"), ern, rows)

  }

  def getSingleReu(rurn: String, ern: String, rows: Seq[HFileRow]): ReportingUnitLink = {
    val reuKey = "REU~" + rurn
    val lurns: Seq[String] = rows.collect {
      case HFileRow(`reuKey`, cells) if (cells.find(cell => cell.value == "LOU").isDefined) => {
        cells.collect { case KVCell(column, "LOU") => column.stripPrefix("c_") } //.toSeq
      }
    }.flatten
    ReportingUnitLink(rurn, lurns, ern)
  }

  def getLinkRecord(ern: String, rows: Seq[HFileRow]): Option[LinkRecord] = {
    val entKey = s"ENT~$ern"
    rows.collect { case HFileRow(`entKey`, cells) => {
      val leus: Seq[LegalUnitLink] = getLeus(ern, rows)
      val rus: Seq[ReportingUnitLink] = getReus(ern, rows)
      LinkRecord(ern, rus, leus)
    }
    }.headOption
  }
}