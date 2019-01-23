package dao.hbase

import dao.DaoUtils._
import model.HFileCell
import org.apache.spark.sql.Row
import util.SequenceGenerator
import util.configuration.AssemblerConfiguration

class HFileUtils {

  val legalUnit = "LEU"
  val localUnit = "LOU"
  val reportingUnit = "REU"
  val enterprise = "ENT"
  val companiesHouse = "CH"
  val vatValue = "VAT"
  val payeValue = "PAYE"

  val childPrefix = "c_"
  val parentPrefix = "p_"


  def rowToAdminData(row: Row): Seq[(String, HFileCell)] = {
    val ern = row.getStringOption("ern").get //must be there

    Seq(
      createEnterpriseCell(ern, "ern", ern)
    ) ++ Seq(
      row.getStringOption("paye_empees").map(employees => createEnterpriseCell(ern, "paye_empees", employees)),
      row.getStringOption("paye_jobs").map(jobs => createEnterpriseCell(ern, "paye_jobs", jobs)),
      row.getStringOption("app_turnover").map(apportion => createEnterpriseCell(ern, "app_turnover", apportion)),
      row.getStringOption("ent_turnover").map(total => createEnterpriseCell(ern, "ent_turnover", total)),
      row.getStringOption("cntd_turnover").map(contained => createEnterpriseCell(ern, "cntd_turnover", contained.toString)),
      row.getStringOption("std_turnover").map(standard => createEnterpriseCell(ern, "std_turnover", standard)),
      row.getStringOption("grp_turnover").map(group => createEnterpriseCell(ern, "grp_turnover", group))

    ).collect { case Some(v) => v }
  }

  def rowToRegion(row: Row): Seq[(String, HFileCell)] = {
    val ern = row.getStringOption("ern").get //must be there
    val region = row.getStringOption("region").getOrElse("0") //must be there

    Seq(
      createEnterpriseCell(ern, "ern", ern),
      createEnterpriseCell(ern, "region", region)
    )
  }

  def rowToEmployment(row: Row): Seq[(String, HFileCell)] = {
    val ern = row.getStringOption("ern").get //must be there
    val employment = row.getStringOption("employment").getOrElse("0") //must be there

    Seq(
      createEnterpriseCell(ern, "ern", ern),
      createEnterpriseCell(ern, "employment", employment)
    )
  }

  def rowToEnt(row: Row): Seq[(String, HFileCell)] = {
    val ern = row.getStringOption("ern").get //must be there
    val prn = row.getStringOption("prn").get //must be there
    val workingProps = row.getStringOption("working_props").getOrElse("0") //must be there
    val employment = row.getStringOption("employment").getOrElse("0") //must be there
    val region = row.getStringOption("region").getOrElse("0") //must be there

    Seq(
      createEnterpriseCell(ern, "ern", ern),
      createEnterpriseCell(ern, "prn", prn),
      createEnterpriseCell(ern, "working_props", workingProps),
      createEnterpriseCell(ern, "employment", employment),
      createEnterpriseCell(ern, "region", region)
    ) ++ Seq(
      row.getStringOption("entref").map(ref => createEnterpriseCell(ern, "entref", ref)),
      row.getStringOption("name").map(name => createEnterpriseCell(ern, "name", name)),
      row.getStringOption("trading_style").map(ls => createEnterpriseCell(ern, "trading_style", ls)),
      row.getStringOption("legal_status").map(ls => createEnterpriseCell(ern, "legal_status", ls)),
      row.getStringOption("address1").map(a1 => createEnterpriseCell(ern, "address1", a1)),
      row.getStringOption("address2").map(a2 => createEnterpriseCell(ern, "address2", a2)),
      row.getStringOption("address3") map (a3 => createEnterpriseCell(ern, "address3", a3)),
      row.getStringOption("address4").map(a4 => createEnterpriseCell(ern, "address4", a4)),
      row.getStringOption("address5") map (a5 => createEnterpriseCell(ern, "address5", a5)),
      row.getStringOption("postcode").map(pc => createEnterpriseCell(ern, "postcode", pc)),
      {
        val sic = row.getStringOption("sic07").getOrElse("")
        Some(createEnterpriseCell(ern, "sic07", sic))
      },
      row.getStringOption("paye_empees").map(employees => createEnterpriseCell(ern, "paye_empees", employees)),
      row.getStringOption("paye_jobs").map(jobs => createEnterpriseCell(ern, "paye_jobs", jobs)),
      row.getStringOption("app_turnover").map(apportion => createEnterpriseCell(ern, "app_turnover", apportion)),
      row.getStringOption("ent_turnover").map(total => createEnterpriseCell(ern, "ent_turnover", total)),
      row.getStringOption("cntd_turnover").map(contained => createEnterpriseCell(ern, "cntd_turnover", contained.toString)),
      row.getStringOption("std_turnover").map(standard => createEnterpriseCell(ern, "std_turnover", standard)),
      row.getStringOption("grp_turnover").map(group => createEnterpriseCell(ern, "grp_turnover", group))

    ).collect { case Some(v) => v }
  }

  private def rowToCHLinks(row: Row, luKey: String, ubrn: String): Seq[(String, HFileCell)] = row.getStringOption("crn").map(companyNo => Seq(
    createLinksRecord(luKey, s"$childPrefix$companyNo", companiesHouse),
    createLinksRecord(generateLinkKey(companyNo, companiesHouse), s"$parentPrefix$legalUnit", ubrn)
  )).getOrElse(Seq[(String, HFileCell)]())

  private def rowToVatRefsLinks(row: Row, luKey: String, ubrn: String): Seq[(String, HFileCell)] = row.getStringSeq("vatrefs").map(_.flatMap(vat => Seq(
    createLinksRecord(luKey, s"$childPrefix$vat", vatValue),
    createLinksRecord(generateLinkKey(vat, vatValue), s"$parentPrefix$legalUnit", ubrn)
  ))).getOrElse(Seq[(String, HFileCell)]())

  private def rowToPayeRefLinks(row: Row, luKey: String, ubrn: String): Seq[(String, HFileCell)] = row.getStringSeq("payerefs").map(_.flatMap(paye => Seq(
    createLinksRecord(luKey, s"$childPrefix$paye", payeValue),
    createLinksRecord(generateLinkKey(paye, payeValue), s"$parentPrefix$legalUnit", ubrn.toString)
  ))).getOrElse(Seq[(String, HFileCell)]())

  private def createLinksRecord(key: String, column: String, value: String): (String, HFileCell) =
    createRecord(key, AssemblerConfiguration.HBaseLinksColumnFamily, column, value)

  private def createEnterpriseCell(ern: String, column: String, value: String): (String, HFileCell) =
    createRecord(generateEntKey(ern), AssemblerConfiguration.HBaseEnterpriseColumnFamily, column, value)

  private def createLocalUnitCell(lurn: String, ern: String, column: String, value: String): (String, HFileCell) =
    createRecord(generateLocalUnitKey(lurn, ern), AssemblerConfiguration.HBaseLocalUnitsColumnFamily, column, value)

  private def createLegalUnitCell(ubrn: String, ern: String, column: String, value: String): (String, HFileCell) =
    createRecord(generateLegalUnitKey(ubrn, ern), AssemblerConfiguration.HBaseLegalUnitsColumnFamily, column, value)

  private def createRecord(key: String, columnFamily: String, column: String, value: String) =
    key -> HFileCell(key, columnFamily, column, value)

  private def generateLocalUnitKey(lurn: String, ern: String) = s"${ern.reverse}~$lurn"

  private def generateLegalUnitKey(ubrn: String, ern: String) = s"${ern.reverse}~$ubrn"

  private def generateEntKey(ern: String) = s"${ern.reverse}"

  private def generateLocalUnitLinksKey(lurn: String) = generateLinkKey(lurn, localUnit)

  private def generateReportingUnitLinksKey(rurn: String) = generateLinkKey(rurn, reportingUnit)

  private def generateLegalUnitLinksKey(ubrn: String) = generateLinkKey(ubrn, legalUnit)

  private def generateLinkKey(id: String, prefix: String): String = s"$prefix~$id"

  def generatePrn(row: Row): String = {
    val rnd = new scala.util.Random
    val numStore = rnd.nextInt(scala.math.pow(2, 31).toInt)
    val prnTest = (numStore % 1000000000) + 1
    "0." + prnTest.toString
  }


  def generateErn(row: Row): String = SequenceGenerator.nextSequence

}

