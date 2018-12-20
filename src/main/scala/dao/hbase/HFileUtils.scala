package dao.hbase

import global.{AppParams, Configs}
import model.hfile.HFileCell
import org.apache.spark.sql.Row
import spark.extensions.sql.SqlRowExtensions
import util.SequenceGenerator

trait HFileUtils extends Serializable {

  val legalUnit = "LEU"
  val localUnit = "LOU"
  val reportingUnit = "REU"
  val enterprise = "ENT"
  val companiesHouse = "CH"
  val vatValue = "VAT"
  val payeValue = "PAYE"

  val childPrefix = "c_"
  val parentPrefix = "p_"

  def rowToEntCalculations(row: Row): Seq[(String, HFileCell)] = {
    val ern = row.getStringOption("ern").get
    val entKey = generateEntKey(ern)

    Seq(
      row.getStringOption("paye_empees").map(employees => createEnterpriseCell(ern, "paye_empees", employees)),
      row.getStringOption("paye_jobs").map(jobs => createEnterpriseCell(ern, "paye_jobs", jobs)),
      row.getStringOption("app_turnover").map(apportion => createEnterpriseCell(ern, "app_turnover", apportion)),
      row.getStringOption("ent_turnover").map(total => createEnterpriseCell(ern, "ent_turnover", total)),
      row.getStringOption("cntd_turnover").map(contained => createEnterpriseCell(ern, "cntd_turnover", contained.toString)),
      row.getStringOption("std_turnover").map(standard => createEnterpriseCell(ern, "std_turnover", standard)),
      row.getStringOption("grp_turnover").map(group => createEnterpriseCell(ern, "grp_turnover", group))
    ).collect { case Some(v) => v }
  }

  def rowToLouCalculations(row: Row): Option[(String, HFileCell)] = {
    val lurn = row.getStringOption("lurn").get
    val ern = row.getStringOption("ern").get
    val entKey = generateLocalUnitKey(lurn, ern)

    row.getStringOption("paye_empees").map(employees => createEnterpriseCell(ern, "employees", employees))
  }

  def entToLinks(row: Row): Seq[(String, HFileCell)] = {
    val ern = row.getStringOption("ern").get
    val entKey = generateEntKey(ern)
    Seq(
      createLinksRecord(entKey, "ern", enterprise)
    )
  }

  def leuToLinks(row: Row): Seq[(String, HFileCell)] = {
    val ubrn = row.getStringOption("ubrn").get
    val ern = row.getStringOption("ern").get
    val entKey = generateLinkKey(ern, enterprise)
    val luKey = generateLinkKey(ubrn, legalUnit)
    val leLinks = rowToLegalUnitLinks(entKey, ubrn, ern)
    val chVatPaye = rowToCHLinks(row, luKey, ubrn) ++ rowToVatRefsLinks(row, luKey, ubrn) ++
      rowToPayeRefLinks(row, luKey, ubrn)
    val all = leLinks ++ chVatPaye
    all
  }

  def louToLinks(row: Row): Seq[(String, HFileCell)] = {
    val lurn = row.getStringOption("lurn").get
    val ern = row.getStringOption("ern").get
    val rurn = row.getStringOption("rurn").get
    val louKey = generateLocalUnitLinksKey(lurn)
    val entKey = generateLinkKey(ern, enterprise)
    val ruKey = generateReportingUnitLinksKey(rurn)

    Seq(
      createLinksRecord(louKey, s"$parentPrefix$enterprise", ern),
      createLinksRecord(louKey, s"$parentPrefix$reportingUnit", rurn),
      createLinksRecord(ruKey, s"$childPrefix$lurn", localUnit),
      createLinksRecord(entKey, s"$childPrefix$lurn", localUnit)
    )
  }

  def ruToLinks(row: Row): Seq[(String, HFileCell)] = {
    val rurn = row.getStringOption("rurn").get
    val ern = row.getStringOption("ern").get
    val ruKey = generateReportingUnitLinksKey(rurn)
    val entKey = generateLinkKey(ern, enterprise)

    Seq(
      createLinksRecord(ruKey, s"$parentPrefix$enterprise", ern),
      createLinksRecord(entKey, s"$childPrefix$rurn", reportingUnit)
    )
  }

  def rowToLegalUnitLinks(entKey: String, ubrn: String, ern: String): Seq[(String, HFileCell)] = {
    val leuKey = generateLegalUnitLinksKey(ubrn)

    Seq(
      createLinksRecord(leuKey, s"$parentPrefix$enterprise", ern),
      createLinksRecord(entKey, s"$childPrefix$ubrn", legalUnit)
    )
  }

  def rowToLocalUnit(row: Row): Seq[(String, HFileCell)] = {
    val lurn = row.getAs[String]("lurn")
    val ern = row.getAs[String]("ern")
    val prn = row.getAs[String]("prn")
    val rurn = row.getAs[String]("rurn")

    Seq(
      createLocalUnitCell(lurn, ern, "lurn", lurn),
      createLocalUnitCell(lurn, ern, "ern", ern),
      createLocalUnitCell(lurn, ern, "prn", prn),
      createLocalUnitCell(lurn, ern, "rurn", rurn),
      createLocalUnitCell(lurn, ern, "name", row.getValueOrEmptyStr("name")),
      createLocalUnitCell(lurn, ern, "address1", row.getValueOrEmptyStr("address1")),
      createLocalUnitCell(lurn, ern, "postcode", row.getValueOrEmptyStr("postcode")),
      createLocalUnitCell(lurn, ern, "region", row.getValueOrEmptyStr("region")),
      createLocalUnitCell(lurn, ern, "sic07", row.getValueOrEmptyStr("sic07")),
      createLocalUnitCell(lurn, ern, "employees", row.getValueOrEmptyStr("employees")),
      createLocalUnitCell(lurn, ern, "employment", row.getValueOrEmptyStr("employment"))
    ) ++ Seq(
      row.getString("ruref").map(bn => createLocalUnitCell(lurn, ern, "ruref", bn)),
      row.getString("luref").map(bn => createLocalUnitCell(lurn, ern, "luref", bn)),
      row.getString("entref").map(bn => createLocalUnitCell(lurn, ern, "entref", bn)),
      row.getString("trading_style").map(bn => createLocalUnitCell(lurn, ern, "trading_style", bn)),
      row.getString("address2").map(bn => createLocalUnitCell(lurn, ern, "address2", bn)),
      row.getString("address3").map(bn => createLocalUnitCell(lurn, ern, "address3", bn)),
      row.getString("address4").map(bn => createLocalUnitCell(lurn, ern, "address4", bn)),
      row.getString("address5").map(bn => createLocalUnitCell(lurn, ern, "address5", bn))
    ).collect { case Some(v) => v }
  }

  def rowToReportingUnit(row: Row): Seq[(String, HFileCell)] = {
    val rurn = row.getStringOption("rurn").get
    val ern = row.getStringOption("ern").get

    Seq(
      createLocalUnitCell(rurn, ern, "ern", ern),
      createLocalUnitCell(rurn, ern, "rurn", rurn),
      createLocalUnitCell(rurn, ern, "name", row.getValueOrEmptyStr("name")),
      createLocalUnitCell(rurn, ern, "address1", row.getValueOrEmptyStr("address1")),
      createLocalUnitCell(rurn, ern, "postcode", row.getValueOrEmptyStr("postcode")),
      createLocalUnitCell(rurn, ern, "region", row.getValueOrEmptyStr("region")),
      createLocalUnitCell(rurn, ern, "sic07", row.getValueOrEmptyStr("sic07")),
      createLocalUnitCell(rurn, ern, "employees", row.getValueOrEmptyStr("employees")), //this one is still long as defined by df schema  of entAdminCalculation
      createLocalUnitCell(rurn, ern, "employment", row.getValueOrEmptyStr("employment")),
      createLocalUnitCell(rurn, ern, "turnover", row.getValueOrEmptyStr("turnover")),
      createLocalUnitCell(rurn, ern, "legal_status", row.getValueOrEmptyStr("legal_status")),
      createLocalUnitCell(rurn, ern, "prn", row.getValueOrEmptyStr("prn"))
    ) ++ Seq(
      row.getString("entref").map(bn => createLocalUnitCell(rurn, ern, "entref", bn)),
      row.getString("ruref").map(bn => createLocalUnitCell(rurn, ern, "ruref", bn)),
      row.getString("trading_style").map(bn => createLocalUnitCell(rurn, ern, "trading_style", bn)),
      row.getString("address2").map(bn => createLocalUnitCell(rurn, ern, "address2", bn)),
      row.getString("address3").map(bn => createLocalUnitCell(rurn, ern, "address3", bn)),
      row.getString("address4").map(bn => createLocalUnitCell(rurn, ern, "address4", bn)),
      row.getString("address5").map(bn => createLocalUnitCell(rurn, ern, "address5", bn))
    ).collect { case Some(v) => v }
  }

  def rowToLegalUnit(row: Row): Seq[(String, HFileCell)] = {
    val lurn = row.getStringOption("ubrn").get
    val ern = row.getStringOption("ern").get
    val prn = row.getStringOption("prn").get

    Seq(
      createLegalUnitCell(lurn, ern, "ubrn", lurn),
      //createLocalUnitCell(lurn,ern, "ern", ern, appParams),
      createLocalUnitCell(lurn, ern, "prn", prn),
      createLocalUnitCell(lurn, ern, "name", row.getString("name").getOrElse("")),
      createLocalUnitCell(lurn, ern, "address1", row.getValueOrEmptyStr("address1")),
      createLocalUnitCell(lurn, ern, "postcode", row.getValueOrEmptyStr("postcode")),
      createLocalUnitCell(lurn, ern, "sic07", row.getValueOrEmptyStr("sic07")),
      createLocalUnitCell(lurn, ern, "legal_status", row.getValueOrEmptyStr("legal_status")),
      createLocalUnitCell(lurn, ern, "birth_date", row.getValueOrEmptyStr("birth_date"))
    ) ++ Seq(
      row.getString("crn").map(bn => createLocalUnitCell(lurn, ern, "crn", bn)),
      row.getString("paye_jobs").map(bn => createLocalUnitCell(lurn, ern, "paye_jobs", bn)),
      row.getString("trading_style").map(bn => createLocalUnitCell(lurn, ern, "trading_style", bn)),
      row.getString("entref").map(bn => createLocalUnitCell(lurn, ern, "entref", bn)),
      row.getString("address2").map(bn => createLocalUnitCell(lurn, ern, "address2", bn)),
      row.getString("address3").map(bn => createLocalUnitCell(lurn, ern, "address3", bn)),
      row.getString("address4").map(bn => createLocalUnitCell(lurn, ern, "address4", bn)),
      row.getString("address5").map(bn => createLocalUnitCell(lurn, ern, "address5", bn)),
      row.getString("turnover").map(bn => createLocalUnitCell(lurn, ern, "turnover", bn)),
      row.getString("trading_status").map(bn => createLocalUnitCell(lurn, ern, "trading_status", bn)),
      row.getString("death_date").map(bn => createLocalUnitCell(lurn, ern, "death_date", bn)),
      row.getString("death_code").map(bn => createLocalUnitCell(lurn, ern, "death_code", bn)),
      row.getString("uprn").map(bn => createLocalUnitCell(lurn, ern, "uprn", bn))
    ).collect { case Some(v) => v }
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

  private def createLinksRecord(key: String, column: String, value: String): (String, HFileCell) = createRecord(key, AppParams.HBASE_LINKS_COLUMN_FAMILY, column, value)

  def createEnterpriseCell(ern: String, column: String, value: String): (String, HFileCell) = createRecord(generateEntKey(ern), AppParams.HBASE_ENTERPRISE_COLUMN_FAMILY, column, value)

  def createLocalUnitCell(lurn: String, ern: String, column: String, value: String): (String, HFileCell) = createRecord(generateLocalUnitKey(lurn, ern), AppParams.HBASE_LOCALUNITS_COLUMN_FAMILY, column, value)

  def createLegalUnitCell(ubrn: String, ern: String, column: String, value: String): (String, HFileCell) = createRecord(generateLegalUnitKey(ubrn, ern), AppParams.HBASE_LEGALUNITS_COLUMN_FAMILY, column, value)

  private def createRecord(key: String, columnFamily: String, column: String, value: String) = key -> HFileCell(key, columnFamily, column, value)

  private def generateLocalUnitKey(lurn: String, ern: String) = s"${ern.reverse}~$lurn"

  private def generateReportingUnitKey(lurn: String, ern: String) = s"${ern.reverse}~$lurn"

  private def generateLegalUnitKey(ubrn: String, ern: String) = s"${ern.reverse}~$ubrn"

  private def generateEntKey(ern: String) = s"${ern.reverse}"

  private def generateLocalUnitLinksKey(lurn: String) = generateLinkKey(lurn, localUnit)

  private def generateReportingUnitLinksKey(rurn: String) = generateLinkKey(rurn, reportingUnit)

  private def generateLegalUnitLinksKey(ubrn: String) = generateLinkKey(ubrn, legalUnit)

  private def generateEntLinkKey(ern: String) = generateLinkKey(ern, enterprise)

  private def generateLinkKey(id: String, prefix: String): String = s"$prefix~$id"

  def getWorkingPropsByLegalStatus(legalStatus: String): String = legalStatus match {
    case "2" => "1"
    case "3" => "2"
    case _ => Configs.DEFAULT_WORKING_PROPS
  }

  def generatePrn(row: Row): String = {
    val rnd = new scala.util.Random
    val numStore = rnd.nextInt(scala.math.pow(2, 31).toInt)
    val prnTest = (numStore % 1000000000) + 1
    "0." + prnTest.toString
  }

  object Sequence extends SequenceGenerator(Configs.config.getString("hbase.zookeper.url"))

  def generateLurn(row: Row): String = Sequence.nextSequence

  def generateErn(row: Row): String = Sequence.nextSequence

  def generateRurn(row: Row): String = Sequence.nextSequence

  def generateLurnFromEnt(row: Row): String = Sequence.nextSequence

}

