package uk.gov.ons.registers.methods

import global.AppParams
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import uk.gov.ons.registers.model.CommonFrameDataFields._

trait VatCalculator{

  def calculateVat(BIDF: DataFrame, payeDF: DataFrame, VatDF: DataFrame)(implicit activeSession: SparkSession ) = {

    val calculatedWithVatAndPaye = joinWithVatAndPayeData(BIDF, VatDF, payeDF)
    val calculatedTurnovers = calculateTurnovers(calculatedWithVatAndPaye)

    aggregateDF(calculatedTurnovers)

  }

  def aggregateDF(df:DataFrame)(implicit spark: SparkSession ) = {
    val t = "CALCULATED"
    df.createOrReplaceTempView(t)

    val sql = s"""
       SELECT *,
       CAST((CASE
           WHEN no_ents_in_group = 1 AND record_type = 1
           THEN turnover
           ELSE NULL
         END
       ) AS long)as $contained,
       CAST((
         CASE
           WHEN no_ents_in_group > 1
           THEN grp_turnover * ($employees / group_empl_total)
           ELSE NULL
         END
       )  AS long)as $apportioned,
       CAST(
       (
         CASE
           WHEN record_type = 0 OR record_type = 2
           THEN turnover
           ELSE NULL
         END
       ) AS long)as $standard
       FROM $t
           """.stripMargin

    val step1 = spark.sql(sql)
    val step1Table = "TEMP"
    step1.createOrReplaceTempView(step1Table)
    val ernByVatGroup = step1.select(col("vat_group"),col("ern"))
    val step2 = step1.select("ern","no_ents_in_group", contained, standard).filter("no_ents_in_group = 1")
    val step2TableName = "STEP2"
    step2.createOrReplaceTempView(step2TableName )
    val aggregateStdAndCntdTurnoversSQL =
      s"""
         SELECT ern,SUM($contained) as $contained,SUM($standard) as $standard
         FROM $step2TableName
         GROUP BY ern
       """.stripMargin

    val step2DF = spark.sql(aggregateStdAndCntdTurnoversSQL)
    /**
      * Sums app_turnovers for every enterprise
      * */
    val app_aggregated = spark.sql(
      s"""SELECT temp.vat_group,temp.ern, SUM(temp.$apportioned) as $apportioned
          FROM (SELECT vat_group, ern,$apportioned from $step1Table GROUP BY vat_group,ern,$apportioned) AS temp
          GROUP BY temp.vat_group,temp.ern""".stripMargin)
    val distinctsTable = "APPAGGREGATED"
    app_aggregated.createOrReplaceTempView(distinctsTable)

    val step3 = step1.drop(contained,standard).join(step2DF, Seq("ern"),"leftouter")
    val aggregated = step3.drop(col(apportioned)).join(app_aggregated,Seq("vat_group","ern"),"leftouter")
    val withAggregatedApp = "WITHAPPAGGR"
    aggregated.createOrReplaceTempView(withAggregatedApp)
    /**
      * /**
      * * ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
      *   paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
      * * */
      * */
    val sql2 = s"SELECT vat_group, ern,$employees, $jobs, $contained, $standard, $apportioned, $group_turnover FROM $withAggregatedApp GROUP BY vat_group, ern, $employees, $jobs, $contained, $standard, $group_turnover, $apportioned "
    val turnovers = spark.sql(sql2)
    val t3 = "TURNOVER"
    turnovers.createOrReplaceTempView(t3)

    val aggregateApportionedSql =
      s"""
         SELECT ern,$employees, $jobs, $contained, SUM($apportioned) as $apportioned, $standard, CAST(SUM($group_turnover) as long) as $group_turnover
         FROM $t3
         GROUP BY ern,$employees, $jobs, $contained,$standard
       """.stripMargin

    val addedEntTurnoverTable = "COMPLETE_TURNOVERS"
    val aggregatedApportionedTurnoverDF = spark.sql(aggregateApportionedSql).drop(col("vat_group"))
    aggregatedApportionedTurnoverDF.createOrReplaceTempView(addedEntTurnoverTable)

    val sql3 =
      s"""SELECT *,
                  ((CASE
                    WHEN $standard is NULL
                    THEN 0
                    ELSE $standard
                  END) +
                 (CASE
                   WHEN $contained is NULL
                   THEN 0
                   ELSE $contained
                 END) +
                    (CASE
                      WHEN $apportioned is NULL
                      THEN 0
                      ELSE $apportioned
                    END)
                     )AS $ent
         FROM $addedEntTurnoverTable""".stripMargin




    val res = spark.sql(sql3)
    res

  }


  def calculateTurnovers(withVatDataSQL:DataFrame)(implicit spark: SparkSession ) = {

    val luTable = "LEGAL_UNITS_WITH_VAT"

    withVatDataSQL.createOrReplaceTempView(luTable)
    val appTurnover = spark.sql(
      s"""
         SELECT t1.*, t2.group_empl_total, t2.no_ents_in_group
         FROM $luTable as t1, ${selectSumEmployees(luTable)} as t2
         WHERE t1.vat_group=t2.vat_group
       """.stripMargin
    )
    val appTurnoverTable = "TURNOVERS"
    appTurnover.createOrReplaceTempView(appTurnoverTable)
    val groupReprTurnovers = spark.sql(groupReprTurnoversSQL(appTurnoverTable))
    appTurnover.join(groupReprTurnovers,Seq("vat_group"),"leftouter")
  }

  def groupReprTurnoversSQL(table:String) = s"SELECT vat_group, turnover as $group_turnover FROM $table WHERE record_type=1 AND no_ents_in_group>1"
  def distinctErnsByPayeEmps(table:String) = s"(SELECT DISTINCT ern, $employees,vat_group FROM $table)"
  def selectSumEmployees(table:String) = s"""(SELECT SUM(emp_total_table.$employees) as group_empl_total, COUNT(emp_total_table.ern) as no_ents_in_group, emp_total_table.vat_group
                                                                                FROM ${distinctErnsByPayeEmps(table)} as emp_total_table
                                                                                GROUP BY emp_total_table.vat_group
                                                              )"""

  def joinWithVatAndPayeData(unitsDF:DataFrame, vatDF:DataFrame, payeCalculatedDF:DataFrame)(implicit spark: SparkSession ) = {
    val flatUnitDf = unitsDF.withColumn("vatref", explode_outer(unitsDF.apply("VatRefs"))).withColumn("vat_group",col("vatref").substr(0,6))

    val luTable = "LEGAL_UNITS"
    val vatTable = "VAT_DATA"
    val payeTable = "PAYE_DATA"

    flatUnitDf.createOrReplaceTempView(luTable)
    vatDF.createOrReplaceTempView(vatTable)
    payeCalculatedDF.createOrReplaceTempView(payeTable)
    val sql = generateWithVatSQL(luTable,payeTable,vatTable)
    val joinedDF = spark.sql(sql)

    joinedDF
  }


  def generateWithVatSQL(luTable:String, payeTable:String, vatTable:String) = {
    s"""SELECT $luTable.vat_group, $luTable.ern,  $luTable.vatref, $vatTable.turnover, $vatTable.record_type, $payeTable.$employees, $payeTable.$jobs
         FROM $luTable, $vatTable, $payeTable
         WHERE $luTable.vatref=$vatTable.vatref AND $payeTable.ern=$luTable.ern""".stripMargin
  }


}
