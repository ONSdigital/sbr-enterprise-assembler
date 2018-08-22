package dao.hbase

import global.{AppParams, Configs}
import model.hfile.HFileCell
import org.apache.spark.sql.Row

import scala.util.{Random, Try}
import spark.extensions.sql.SqlRowExtensions



trait HFileUtils extends Serializable{

  val legalUnit = "LEU"
  val localUnit = "LOU"
  val enterprise = "ENT"
  val companiesHouse = "CH"
  val vatValue = "VAT"
  val payeValue = "PAYE"

  val childPrefix = "c_"
  val parentPrefix = "p_"


  def rowToEntCalculations(row:Row,appParams:AppParams) = {
    val ern = getString(row,"ern").get
    val entKey = generateEntKey(ern,appParams)
    Seq(
          getString(row,"paye_empees").map(employees => createEnterpriseCell(ern, "paye_empees", employees, appParams)),
          getString(row,"paye_jobs").map(jobs => createEnterpriseCell(ern, "paye_jobs", jobs, appParams)),
          getString(row,"app_turnover").map(apportion => createEnterpriseCell(ern, "app_turnover", apportion, appParams)),
          getString(row,"ent_turnover").map(total => createEnterpriseCell(ern, "ent_turnover", total, appParams)),
          getString(row,"cntd_turnover").map(contained => createEnterpriseCell(ern, "cntd_turnover", contained.toString, appParams)),
          getString(row,"std_turnover").map(standard => createEnterpriseCell(ern, "std_turnover", standard, appParams)),
          getString(row,"grp_turnover").map(group => createEnterpriseCell(ern, "grp_turnover", group, appParams))
      ).collect { case Some(v) => v }
  }


  def rowToLouCalculations(row:Row,appParams:AppParams) = {
    val lurn = getString(row,"lurn").get
    val ern = getString(row,"ern").get
    val entKey = generateLocalUnitKey(lurn,ern,appParams)
    getString(row,"paye_empees").map(employees => createEnterpriseCell(ern, "employees", employees, appParams))
  }

  def entToLinks(row:Row,appParams:AppParams):Seq[(String, HFileCell)] = {
    val ern = getString(row,"ern").get
    val entKey = generateEntKey(ern,appParams)
    Seq(
      createLinksRecord(entKey,"ern",enterprise,appParams)
    )
  }

  def leuToLinks(row: Row, appParams: AppParams) = {
    val ubrn = getString(row,"id").get
    val ern = getString(row,"ern").get
    val entKey = generateLinkKey(ern,enterprise)
    val luKey = generateLinkKey(ubrn,legalUnit)
    val leLinks = rowToLegalUnitLinks(entKey,ubrn,ern,appParams)
    val chVatPaye = (rowToCHLinks(row,luKey,ubrn,appParams) ++ rowToVatRefsLinks(row,luKey,ubrn,appParams) ++ rowToPayeRefLinks(row,luKey,ubrn,appParams))
    val all = leLinks ++ chVatPaye
    all
  }

  def louToLinks(row:Row,appParams:AppParams):Seq[(String, HFileCell)] = {
    val lurn = getString(row,"lurn").get
    val ern = getString(row,"ern").get
    val loKey = generateLocalUnitLinksKey(lurn)
    val entKey = generateLinkKey(ern,enterprise)
    Seq(
      createLinksRecord(loKey,s"$parentPrefix$enterprise",ern,appParams),
      createLinksRecord(entKey,s"$childPrefix$lurn",localUnit,appParams)
    )
  }

  def ruToLinks(row:Row,appParams:AppParams):Seq[(String, HFileCell)] = {
    val lurn = getString(row,"lurn").get
    val ern = getString(row,"ern").get
    val loKey = generateLocalUnitLinksKey(lurn)
    val entKey = generateLinkKey(ern,enterprise)
    Seq(
      createLinksRecord(loKey,s"$parentPrefix$enterprise",ern,appParams),
      createLinksRecord(entKey,s"$childPrefix$lurn",localUnit,appParams)
    )
  }


  def rowToLegalUnitLinks(entKey:String,ubrn:String, ern:String,appParams:AppParams):Seq[(String, HFileCell)] = {
    val leuKey = generateLegalUnitLinksKey(ubrn)
    Seq(
      createLinksRecord(leuKey,s"$parentPrefix$enterprise",ern,appParams),
      createLinksRecord(entKey,s"$childPrefix$ubrn",legalUnit,appParams)
    )
  }

  def rowToLocalUnit(row: Row, appParams: AppParams):Seq[(String, HFileCell)] = {
    val lurn = getString(row,"lurn").get
    val ern = getString(row,"ern").get
    Seq(
      createLocalUnitCell(lurn,ern, "lurn", lurn, appParams),
      createLocalUnitCell(lurn,ern, "ern", ern, appParams),
      createLocalUnitCell(lurn,ern, "name", row.getString("name").getOrElse(""), appParams),
      createLocalUnitCell(lurn,ern, "address1", row.getString("address1").getOrElse(""), appParams),
      createLocalUnitCell(lurn,ern, "postcode", row.getString("postcode").getOrElse(""), appParams),
      createLocalUnitCell(lurn,ern, "sic07", row.getString("sic07").getOrElse(""), appParams),
      createLocalUnitCell(lurn,ern, "employees", row.getString("employees").map(_.toString).getOrElse(""), appParams) //this one is still long as defined by df schema  of entAdminCalculation
    ) ++ Seq(
      row.getString("luref").map(bn => createLocalUnitCell(lurn,ern, "luref", bn, appParams)),
      row.getString("entref").map(bn => createLocalUnitCell(lurn,ern, "entref", bn, appParams)),
      row.getString("trading_style").map(bn => createLocalUnitCell(lurn,ern, "trading_style", bn, appParams)),
      row.getString("address2").map(bn => createLocalUnitCell(lurn,ern, "address2", bn, appParams)),
      row.getString("address3").map(bn => createLocalUnitCell(lurn,ern, "address3", bn, appParams)),
      row.getString("address4").map(bn => createLocalUnitCell(lurn,ern, "address4", bn, appParams)),
      row.getString("address5").map(bn => createLocalUnitCell(lurn,ern, "address5", bn, appParams))
    ).collect { case Some(v) => v }
  }


  def rowToLegalUnit(row: Row, appParams: AppParams):Seq[(String, HFileCell)] = {
    val lurn = getString(row,"ubrn").get
    val ern = getString(row,"ern").get
    Seq(
      createLegalUnitCell(lurn,ern, "ubrn", lurn, appParams),
      createLocalUnitCell(lurn,ern, "ern", ern, appParams),
      createLocalUnitCell(lurn,ern, "name", row.getString("name").getOrElse(""), appParams),
      createLocalUnitCell(lurn,ern, "address1", getValueOrEmptyStr(row,"address1"), appParams),
      createLocalUnitCell(lurn,ern, "postcode", getValueOrEmptyStr(row,"postcode"), appParams),
      createLocalUnitCell(lurn,ern, "sic07", getValueOrEmptyStr(row,"sic07"), appParams),
      createLocalUnitCell(lurn,ern, "legal_status", getValueOrEmptyStr(row,"legal_status"), appParams) ,
      createLocalUnitCell(lurn,ern, "birth_date", getValueOrEmptyStr(row,"birth_date"), appParams)
    ) ++ Seq(
      row.getString("crn").map(bn => createLocalUnitCell(lurn,ern, "crn", bn, appParams)),
      row.getString("paye_jobs").map(bn => createLocalUnitCell(lurn,ern, "paye_jobs", bn, appParams)),
      row.getString("trading_style").map(bn => createLocalUnitCell(lurn,ern, "trading_style", bn, appParams)),
      row.getString("entref").map(bn => createLocalUnitCell(lurn,ern, "entref", bn, appParams)),
      row.getString("address2").map(bn => createLocalUnitCell(lurn,ern, "address2", bn, appParams)),
      row.getString("address3").map(bn => createLocalUnitCell(lurn,ern, "address3", bn, appParams)),
      row.getString("address4").map(bn => createLocalUnitCell(lurn,ern, "address4", bn, appParams)),
      row.getString("address5").map(bn => createLocalUnitCell(lurn,ern, "address5", bn, appParams)),
      row.getString("turnover").map(bn => createLocalUnitCell(lurn,ern, "turnover", bn, appParams)),
      row.getString("trading_status").map(bn => createLocalUnitCell(lurn,ern, "trading_status", bn, appParams)),
      row.getString("death_date").map(bn => createLocalUnitCell(lurn,ern, "death_date", bn, appParams)),
      row.getString("death_code").map(bn => createLocalUnitCell(lurn,ern, "death_code", bn, appParams)),
      row.getString("uprn").map(bn => createLocalUnitCell(lurn,ern, "uprn", bn, appParams))
    ).collect { case Some(v) => v }
  }

  def rowToEnt(row: Row, appParams: AppParams): Seq[(String, HFileCell)] = {
    val ern = getString(row,"ern").get //must be there
    val prn = getString(row,"prn").get //must be there
    Seq(createEnterpriseCell(ern, "ern", ern, appParams),createEnterpriseCell(ern, "prn", prn, appParams)) ++
      Seq(
        getString(row,"entref").map(ref => createEnterpriseCell(ern, "entref", ref, appParams)),
        getString(row,"name").map (name => createEnterpriseCell(ern, "name", name, appParams)),
        getString(row,"trading_style").map(ls => createEnterpriseCell(ern, "trading_style", ls, appParams)),
        getString(row,"legal_status").map(ls => createEnterpriseCell(ern, "legal_status", ls, appParams)),
        getString(row,"address1").map (a1 => createEnterpriseCell(ern, "address1", a1, appParams)),
        getString(row,"address2").map(a2 => createEnterpriseCell(ern, "address2", a2, appParams)),
        getString(row,"address3") map (a3 => createEnterpriseCell(ern, "address3", a3, appParams)),
        getString(row,"address4").map(a4 => createEnterpriseCell(ern, "address4", a4, appParams)),
        getString(row,"address5") map (a5 => createEnterpriseCell(ern, "address5", a5, appParams)),
        getString(row,"postcode").map(pc => createEnterpriseCell(ern, "postcode", pc, appParams)),
        {
          val sic = getString(row,"sic07").getOrElse("")
          Some(createEnterpriseCell(ern, "sic07", sic, appParams))
        },
        getString(row,"paye_empees").map(employees => createEnterpriseCell(ern, "paye_empees", employees, appParams)),
        getString(row,"paye_jobs").map(jobs => createEnterpriseCell(ern, "paye_jobs", jobs, appParams)),
        getString(row,"app_turnover").map(apportion => createEnterpriseCell(ern, "app_turnover", apportion, appParams)),
        getString(row,"ent_turnover").map(total => createEnterpriseCell(ern, "ent_turnover", total, appParams)),
        getString(row,"cntd_turnover").map(contained => createEnterpriseCell(ern, "cntd_turnover", contained.toString, appParams)),
        getString(row,"std_turnover").map(standard => createEnterpriseCell(ern, "std_turnover", standard, appParams)),
        getString(row,"grp_turnover").map(group => createEnterpriseCell(ern, "grp_turnover", group, appParams))
      ).collect { case Some(v) => v }
  }

  private def rowToCHLinks(row:Row, luKey:String, ubrn:String,appParams:AppParams):Seq[(String, HFileCell)] = getString(row,"CompanyNo").map(companyNo => Seq(
    createLinksRecord(luKey,s"$childPrefix$companyNo",companiesHouse,appParams),
    createLinksRecord(generateLinkKey(companyNo,companiesHouse),s"$parentPrefix$legalUnit",ubrn,appParams)
  )).getOrElse(Seq[(String, HFileCell)]())

  private def rowToVatRefsLinks(row:Row, luKey:String, ubrn:String,appParams:AppParams):Seq[(String, HFileCell)] = row.getStringSeq("VatRefs").map(_.flatMap(vat => Seq(
    createLinksRecord(luKey,s"$childPrefix$vat",vatValue,appParams),
    createLinksRecord(generateLinkKey(vat,vatValue),s"$parentPrefix$legalUnit",ubrn,appParams)
  ))).getOrElse (Seq[(String, HFileCell)]())

  private def rowToPayeRefLinks(row:Row, luKey:String, ubrn:String,appParams:AppParams):Seq[(String, HFileCell)] = row.getStringSeq("PayeRefs").map(_.flatMap(paye => Seq(
    createLinksRecord(luKey,s"$childPrefix$paye",payeValue,appParams),
    createLinksRecord(generateLinkKey(paye,payeValue),s"$parentPrefix$legalUnit",ubrn.toString,appParams)
  ))).getOrElse(Seq[(String, HFileCell)]())

  private def createLinksRecord(key:String,column:String, value:String, appParams:AppParams) = createRecord(key,appParams.HBASE_LINKS_COLUMN_FAMILY,column,value)

  def createEnterpriseCell(ern:String,column:String, value:String, appParams:AppParams) = createRecord(generateEntKey(ern,appParams),appParams.HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  def createLocalUnitCell(lurn:String,ern:String,column:String, value:String, appParams:AppParams) = createRecord(generateLocalUnitKey(lurn,ern,appParams),appParams.HBASE_LOCALUNITS_COLUMN_FAMILY,column,value)

  def createLegalUnitCell(ubrn:String,ern:String,column:String, value:String, appParams:AppParams) = createRecord(generateLegalUnitKey(ubrn,ern,appParams),appParams.HBASE_LEGALUNITS_COLUMN_FAMILY,column,value)

  private def createRecord(key:String,columnFamily:String, column:String, value:String) = key -> HFileCell(key,columnFamily,column,value)

  private def generateLocalUnitKey(lurn:String,ern:String,appParams:AppParams) = s"${ern.reverse}~$lurn"

  private def generateLegalUnitKey(ubrn:String,ern:String,appParams:AppParams) = s"${ern.reverse}~$ubrn"

  private def generateEntKey(ern:String,appParams:AppParams) = s"${ern.reverse}"

  private def generateLocalUnitLinksKey(lurn:String) = generateLinkKey(lurn,localUnit)

  private def generateLegalUnitLinksKey(ubrn:String) = generateLinkKey(ubrn,legalUnit)

  private def generateEntLinkKey(ern:String) = generateLinkKey(ern,enterprise)

  private def generateLinkKey(id:String, prefix:String) = s"$prefix~$id"


  def getValueOrEmptyStr(row:Row, fieldName:String) = Try{
    val value = row.getAs[String](fieldName)
    value.size //just to trigger NullPointerException if is null
    value
  }.getOrElse("")

  def getValueOrNull(row:Row, fieldName:String) = Try{
    val value = row.getAs[String](fieldName)
    value.size //just to trigger NullPointerException if is null
    value
  }.getOrElse(null)

  /**
    * Returns None if:
    * 1. row does not contain field with given name
    * 2. value is null
    * 3. value's data type is not String
    * returns Some(...) of value O
    * otherwise
    * */
  def getString(row:Row,name:String) = {
    Try{
      val value = row.getAs[String](name)
      if(value==null) {
        throw new NullPointerException(s"value of field $name is null, None will be returned")
      }
      else value
    }.toOption
  }

  def generateErn(row:Row, appParams:AppParams) = generateUniqueKey
  def generateRurn(row:Row, appParams:AppParams) = generateUniqueKey
  def generatePrn(row:Row, appParams:AppParams) = (Configs.DEFAULT_PRN.toDouble + 0.001D).toString
  def generateLurn(row:Row, appParams:AppParams) = generateUniqueKey
  def generateLurnFromEnt(row:Row, appParams:AppParams) = generateUniqueKey

  def generateUniqueKey = "N"+Random.alphanumeric.take(17).mkString
}
