package dao.hbase.converter


import global.Configs
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
trait WithConvertionHelper {
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
    val latest = "dec_jobs"

  def toRecords(row:Row,timePeriod:String): Tables = {
    val ern = generateErn
    Tables(rowToEnterprise(row,ern,timePeriod),rowToLinks(row,ern,timePeriod))
  }

  private def rowToEnterprise(row:Row,ern:String,timePeriod:String): Seq[(String, RowObject)] = Seq(createEnterpriseRecord(ern,"ern",ern,timePeriod), createEnterpriseRecord(ern,"idbrref","9999999999",timePeriod))++
        Seq(
          row.getString("BusinessName").map(bn  => createEnterpriseRecord(ern,"name",bn,timePeriod)),
          row.getString("PostCode")map(pc => createEnterpriseRecord(ern,"postcode",pc,timePeriod)),
          row.getString("LegalStatus").map(ls => createEnterpriseRecord(ern,"legalstatus",ls,timePeriod)),
          row.getInt("avg").map(avg => createEnterpriseRecord(ern,"avg",avg.toString,timePeriod)),
          row.getLong(s"sum($latest)").map(sum => createEnterpriseRecord(ern,"latestSum",sum.toString,timePeriod))
        ).collect{case Some(v) => v}

  private def rowToLinks(row:Row,ern:String,timePeriod:String): Seq[(String, RowObject)] = {
      val ubrn = getId(row)
      val keyStr = generateLinkKey(ern,enterprise,timePeriod)
      createLinksRecord(keyStr,s"$childPrefix$ubrn",legalUnit)+:rowToLegalUnitLinks(row,ern,timePeriod)
    }

  private def rowToLegalUnitLinks(row:Row, ern:String,timePeriod:String):Seq[(String, RowObject)] = {
      val ubrn = getId(row)
      val luKey = generateLinkKey(ubrn,legalUnit,timePeriod)
      createLinksRecord(luKey,s"$parentPrefix$enterprise",ern) +: (rowToCHLinks(row,luKey,ubrn,timePeriod) ++ rowToVatRefsLinks(row,luKey,ubrn,timePeriod) ++ rowToPayeRefLinks(row,luKey,ubrn,timePeriod))
    }

  private def rowToCHLinks(row:Row, luKey:String, ubrn:String,timePeriod:String):Seq[(String, RowObject)] = row.getString("CompanyNo").map(companyNo => Seq(
      createLinksRecord(luKey,s"$childPrefix$companyNo",companiesHouse),
      createLinksRecord(generateLinkKey(companyNo,companiesHouse,timePeriod),s"$parentPrefix$legalUnit",ubrn)
    )).getOrElse(Seq[(String, RowObject)]())

  private def rowToVatRefsLinks(row:Row, luKey:String, ubrn:String,timePeriod:String):Seq[(String, RowObject)] = row.getLongSeq("VatRefs").map(_.flatMap(vat => Seq(
        createLinksRecord(luKey,s"$childPrefix$vat",vatValue),
        createLinksRecord(generateLinkKey(vat.toString,vatValue,timePeriod),s"$parentPrefix$legalUnit",ubrn.toString)
      ))).getOrElse (Seq[(String, RowObject)]())


  private def rowToPayeRefLinks(row:Row, luKey:String, ubrn:String,timePeriod:String):Seq[(String, RowObject)] = row.getStringSeq("PayeRefs").map(_.flatMap(paye => Seq(
        createLinksRecord(luKey,s"$childPrefix$paye",payeValue),
        createLinksRecord(generateLinkKey(paye,payeValue,timePeriod),s"$parentPrefix$legalUnit",ubrn.toString)
      ))).getOrElse(Seq[(String, RowObject)]())

  private def getId(row:Row) = row.getLong("id").map(_.toString).getOrElse(throw new IllegalArgumentException("id must be present"))

  private def createLinksRecord(key:String,column:String, value:String) = createRecord(key,HBASE_LINKS_COLUMN_FAMILY,column,value)

  private def createEnterpriseRecord(ern:String,column:String, value:String,timePeriod:String) = createRecord(generateEntKey(ern,timePeriod),HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  private def createRecord(key:String,columnFamily:String, column:String, value:String) = key -> RowObject(key,columnFamily,column,value)

  private def generateErn = Random.alphanumeric.take(18).mkString

  private def generateEntKey(ern:String,timePeriod:String) = {
    s"${ern.reverse}~$timePeriod"
  }

  private def generateLinkKey(id:String, suffix:String,timePeriod:String) = {
    s"$id~$suffix~$timePeriod"
  }
}
