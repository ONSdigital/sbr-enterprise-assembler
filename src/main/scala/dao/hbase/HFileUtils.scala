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

  private def createLinksRecord(key: String, column: String, value: String): (String, HFileCell) =
    createRecord(key, AssemblerConfiguration.HBaseLinksColumnFamily, column, value)

  private def createEnterpriseCell(ern: String, column: String, value: String): (String, HFileCell) =
    createRecord(generateEntKey(ern), AssemblerConfiguration.HBaseEnterpriseColumnFamily, column, value)

  private def createRecord(key: String, columnFamily: String, column: String, value: String) =
    key -> HFileCell(key, columnFamily, column, value)

  private def generateEntKey(ern: String) = s"${ern.reverse}"

  def generatePrn(row: Row): String = {
    val rnd = new scala.util.Random
    val numStore = rnd.nextInt(scala.math.pow(2, 31).toInt)
    val prnTest = (numStore % 1000000000) + 1
    "0." + prnTest.toString
  }


  def generateErn(row: Row): String = SequenceGenerator.nextSequence

}

