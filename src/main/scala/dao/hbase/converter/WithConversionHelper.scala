package dao.hbase.converter


import global.{AppParams, Configs}
import model.hfile._
import org.apache.spark.sql.Row

import scala.util.Random
import spark.extensions.sql.SqlRowExtensions
/**
  * Schema:
  * index | fields
  * -------------------
  *  0 -   BusinessName: string (nullable = true)
  *  1 -   CompanyNo: string (nullable = true)
  *  2 -   EmploymentBands: string (nullable = true)
  *  3 -   IndustryCode: string (nullable = true)
  *  4 -   LegalStatus: string (nullable = true)
  *  5 -   PayeRefs: array (nullable = true)
  *  6 -   PostCode: string (nullable = true)
  *  7 -   TradingStatus: string (nullable = true)
  *  8 -   Turnover: string (nullable = true)
  *  9 -   UPRN: long (nullable = true)
  *  10 -  VatRefs: array (nullable = true)
  *  11 -  id: long (nullable = true)
  */
trait WithConversionHelper {
  /*
  * Rules:
  * fields needed for creating ENTERPRISE UNIT:
  * 1. ID(UBRN) - NOT NULL
  * ## At least one of the below must be present
  * 2. PayeRefs  - NULLABLE
  * 3. VatRefs - NULLABLE
  * 4. CompanyNo - NULLABLE
  * */

  import Configs._

  val legalUnit = "LEU"
  val enterprise = "ENT"
  val companiesHouse = "CH"
  val vatValue = "VAT"
  val payeValue = "PAYE"

  val childPrefix = "c_"
  val parentPrefix = "p_"

  def toEnterpriseRecords(row: Row, appParams: AppParams): Tables = {
    val ern = generateErn
    Tables(rowToFullEnterprise(row, appParams, ern), rowToLinks(row, ern, appParams))
  }

  def toNewEnterpriseRecords(row: Row, appParams: AppParams): Tables = {
    val ern = generateErn
    Tables(rowToEnterprise(row, ern, appParams), rowToLinks(row, ern, appParams))
  }

  def toLinksRefreshRecords(row: Row, appParams: AppParams): Seq[(String, HFileCell)] = {
    val ubrn = getId(row, "id")
    val luKey = generateLinkKey(ubrn, legalUnit, appParams)

    (rowToCHLinks(row, luKey, ubrn, appParams) ++ rowToVatRefsLinks(row, luKey, ubrn, appParams) ++ rowToPayeRefLinks(row, luKey, ubrn, appParams))
  }


  def toLuRecords(row: Row, appParams: AppParams): Seq[(String, HFileCell)] = {
    val ubrn = getId(row, "id")
    val luKey = generateLinkKey(ubrn, legalUnit, appParams)
    (rowToCHLinks(row, luKey, ubrn, appParams) ++ rowToVatRefsLinks(row, luKey, ubrn, appParams) ++ rowToPayeRefLinks(row, luKey, ubrn, appParams))
  }

  def rowToEnterprise(row: Row, appParams: AppParams): Seq[(String, HFileCell)] = {
    val ern = row.getString("ern").get //must be present
    rowToEnterprise(row, ern, appParams)
  }


  def rowToEnterprise(row: Row, ern: String, appParams: AppParams): Seq[(String, HFileCell)] = Seq(createEnterpriseCell(ern, "ern", ern, appParams), createEnterpriseCell(ern, "entref", "9999999999", appParams)) ++
    Seq(
      row.getString("BusinessName").map(bn => createEnterpriseCell(ern, "name", bn, appParams)),
      row.getString("PostCode") map (pc => createEnterpriseCell(ern, "postcode", pc, appParams)),
      row.getString("LegalStatus").map(ls => createEnterpriseCell(ern, "legalstatus", ls, appParams)),
      row.getString("IndustryCode").map(sic => createEnterpriseCell(ern, "sic07", sic, appParams)),
      row.getCalcValue("paye_employees").map(employees => createEnterpriseCell(ern, "paye_employees", employees, appParams)),
      row.getCalcValue("paye_jobs").map(jobs => createEnterpriseCell(ern, "paye_jobs", jobs, appParams)),
      row.getCalcValue("apportion_turnover").map(apportion => createEnterpriseCell(ern, "apportion_turnover", apportion, appParams)),
      row.getCalcValue("total_turnover").map(total => createEnterpriseCell(ern, "total_turnover", total, appParams)),
      row.getCalcValue("temp_contained_rep_vat_turnover").map(contained => createEnterpriseCell(ern, "contained_turnover", contained, appParams)),
      row.getCalcValue("temp_standard_vat_turnover").map(standard => createEnterpriseCell(ern, "standard_turnover", standard, appParams)),
      row.getCalcValue("group_turnover").map(group => createEnterpriseCell(ern, "group_turnover", group, appParams))
    ).collect { case Some(v) => v }

/**/
  def rowToFullEnterprise(row: Row, appParams: AppParams, ern:String): Seq[(String, HFileCell)] = {

    Seq(createEnterpriseCell(ern, "ern", ern, appParams)) ++
    Seq(
      row.getString("entref").map(ref => createEnterpriseCell(ern, "entref", ref, appParams)),
      row.getString("name") map (name => createEnterpriseCell(ern, "name", name, appParams)),
      row.getString("tradingstyle").map(ls => createEnterpriseCell(ern, "tradingstyle", ls, appParams)),
      row.getString("address1") map (a1 => createEnterpriseCell(ern, "address1", a1, appParams)),
      row.getString("address2").map(a2 => createEnterpriseCell(ern, "address2", a2, appParams)),
      row.getString("address3") map (a3 => createEnterpriseCell(ern, "address3", a3, appParams)),
      row.getString("address4").map(a4 => createEnterpriseCell(ern, "address4", a4, appParams)),
      row.getString("address5") map (a5 => createEnterpriseCell(ern, "address5", a5, appParams)),
      row.getString("postcode").map(pc => createEnterpriseCell(ern, "postcode", pc, appParams)),
      row.getString("sic07").map(sic => createEnterpriseCell(ern, "sic07", sic, appParams)),
      row.getString("legalstatus").map(ls => createEnterpriseCell(ern, "legalstatus", ls, appParams)),
      row.getCalcValue("paye_employees").map(employees => createEnterpriseCell(ern, "paye_employees", employees, appParams)),
      row.getCalcValue("paye_jobs").map(jobs => createEnterpriseCell(ern, "paye_jobs", jobs, appParams)),
      row.getCalcValue("apportion_turnover").map(apportion => createEnterpriseCell(ern, "apportion_turnover", apportion, appParams)),
      row.getCalcValue("total_turnover").map(total => createEnterpriseCell(ern, "total_turnover", total, appParams)),
      row.getCalcValue("temp_contained_rep_vat_turnover").map(contained => createEnterpriseCell(ern, "contained_turnover", contained, appParams)),
      row.getCalcValue("temp_standard_vat_turnover").map(standard => createEnterpriseCell(ern, "standard_turnover", standard, appParams)),
      row.getCalcValue("group_turnover").map(group => createEnterpriseCell(ern, "group_turnover", group, appParams))
    ).collect { case Some(v) => v }
}

/**/
  def rowToFullEnterprise(row: Row, appParams: AppParams): Seq[(String, HFileCell)] = {
    val ern = row.getString("ern").get //must be there

    Seq(createEnterpriseCell(ern, "ern", ern, appParams)) ++
    rowToFullEnterprise(row,appParams,ern)
}



  private def rowToLinks(row:Row,ern:String,appParams:AppParams): Seq[(String, HFileCell)] = {
    val ubrn = getId(row,"id")
    val keyStr = generateLinkKey(ern,enterprise,appParams)
    createLinksRecord(keyStr,s"$childPrefix$ubrn",legalUnit,appParams)+:rowToLegalUnitLinks(row,ern,appParams)
  }

  def rowToLegalUnitLinks(row:Row, ern:String,appParams:AppParams):Seq[(String, HFileCell)] = rowToLegalUnitLinks("id",ern,row,appParams)


  def rowToLegalUnitLinks(row:Row, appParams:AppParams):Seq[(String, HFileCell)] = rowToLegalUnitLinks("id",row,appParams)



  def rowToLegalUnitLinks(idField:String,row:Row,appParams:AppParams):Seq[(String, HFileCell)] = {
    val ubrn = getId(row,idField)
    val luKey = generateLinkKey(ubrn,legalUnit,appParams)
    val ern = row.getString("ern").get //must be present
    createLinksRecord(luKey,s"$parentPrefix$enterprise",ern,appParams) +: (rowToCHLinks(row,luKey,ubrn,appParams) ++ rowToVatRefsLinks(row,luKey,ubrn,appParams) ++ rowToPayeRefLinks(row,luKey,ubrn,appParams))
  }


  def rowToLegalUnitLinks(idField:String,ern:String,row:Row,appParams:AppParams):Seq[(String, HFileCell)] = {
    val ubrn = getId(row,idField)
    val luKey = generateLinkKey(ubrn,legalUnit,appParams)
    createLinksRecord(luKey,s"$parentPrefix$enterprise",ern,appParams) +: (rowToCHLinks(row,luKey,ubrn,appParams) ++ rowToVatRefsLinks(row,luKey,ubrn,appParams) ++ rowToPayeRefLinks(row,luKey,ubrn,appParams))
  }




  private def rowToCHLinks(row:Row, luKey:String, ubrn:String,appParams:AppParams):Seq[(String, HFileCell)] = row.getString("CompanyNo").map(companyNo => Seq(
    createLinksRecord(luKey,s"$childPrefix$companyNo",companiesHouse,appParams),
    createLinksRecord(generateLinkKey(companyNo,companiesHouse,appParams),s"$parentPrefix$legalUnit",ubrn,appParams)
  )).getOrElse(Seq[(String, HFileCell)]())

  private def rowToVatRefsLinks(row:Row, luKey:String, ubrn:String,appParams:AppParams):Seq[(String, HFileCell)] = row.getLongSeq("VatRefs").map(_.flatMap(vat => Seq(
    createLinksRecord(luKey,s"$childPrefix$vat",vatValue,appParams),
    createLinksRecord(generateLinkKey(vat.toString,vatValue,appParams),s"$parentPrefix$legalUnit",ubrn.toString,appParams)
  ))).getOrElse (Seq[(String, HFileCell)]())

  private def rowToPayeRefLinks(row:Row, luKey:String, ubrn:String,appParams:AppParams):Seq[(String, HFileCell)] = row.getStringSeq("PayeRefs").map(_.flatMap(paye => Seq(
    createLinksRecord(luKey,s"$childPrefix$paye",payeValue,appParams),
    createLinksRecord(generateLinkKey(paye,payeValue,appParams),s"$parentPrefix$legalUnit",ubrn.toString,appParams)
  ))).getOrElse(Seq[(String, HFileCell)]())

  //def getId(row:Row) = row.getLong("id").map(_.toString).getOrElse(throw new IllegalArgumentException("id must be present"))

  def getId(row:Row,idField:String) = row.getLong(idField).map(_.toString).getOrElse(throw new IllegalArgumentException("id must be present"))

  private def createLinksRecord(key:String,column:String, value:String, appParams:AppParams) = createRecord(key,appParams.HBASE_LINKS_COLUMN_FAMILY,column,value)

  def createEnterpriseCell(ern:String,column:String, value:String, appParams:AppParams) = createRecord(generateEntKey(ern,appParams),appParams.HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  private def createRecord(key:String,columnFamily:String, column:String, value:String) = key -> HFileCell(key,columnFamily,column,value)

  private def generateErn = Random.alphanumeric.take(18).mkString

  private def generateEntKey(ern:String,appParams:AppParams) = {
    s"${ern.reverse}~${appParams.TIME_PERIOD}"
  }

  private def generateLinkKey(id:String, suffix:String, appParams:AppParams) = {
    s"$id~$suffix~${appParams.TIME_PERIOD}"
  }
}
