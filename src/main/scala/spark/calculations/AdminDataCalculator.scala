package spark.calculations

import global.AppParams
import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.RddLogging


trait AdminDataCalculator extends Serializable with RddLogging{
//paye_empees, $jobs, app_turnover, $ent, cntd_turnover, std_turnover, grp_turnover
  val jobs = "paye_jobs"
  val employees = "paye_empees"
  val apportioned = "app_turnover"
  val contained = "cntd_turnover"
  val ent = "ent_turnover"
  val standard = "std_turnover"
  val group = "grp_turnover"
  
  def calculate(unitsDF:DataFrame,appConfs:AppParams)(implicit spark: SparkSession ) = {
    val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
    val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
    //unitsDF.show()
    val calculatedPayeDF = getGroupedByPayeRefs(unitsDF,payeDF,"dec_jobs")
    //printDF("calculatedPayeDF",calculatedPayeDF)
    val calculatedWithVatAndPaye = calculateGroupTurnover(unitsDF,vatDF,calculatedPayeDF)
    //printDF("calculatedWithVatAndPaye",calculatedWithVatAndPaye)
    val calculatedTurnovers = calculateTurnovers(calculatedWithVatAndPaye,vatDF)
    //printDF("calculatedTurnovers",calculatedTurnovers)
    val res = aggregateDF(calculatedTurnovers)
    //printDF("res",res)
    res

  }

  def aggregateDF(df:DataFrame)(implicit spark: SparkSession ) = {//TODO:
    /**
      *
      * to be added another aggregation on top og this:
      * +----------+-----------+---------+-------------+------------+------------+------------+------------+
      * |       ern|paye_empees|paye_jobs|cntd_turnover|app_turnover|std_turnover|grp_turnover|ent_turnover|
      * +----------+-----------+---------+-------------+------------+------------+------------+------------+
      * |2000000011|          2|        4|         null|        null|         390|        null|         390|
      * |1100000004|          4|        8|         null|        null|         260|        null|         260|
      * |2200000002|          5|        5|         null|        null|        null|         444|           0|
      * |9900000009|       null|     null|         null|        null|          85|        null|          85|
      * |1100000004|          4|        8|         null|         444|        null|         444|         444|
      * |1100000003|         19|       20|          585|        null|        null|        null|         585|
      * +----------+-----------+---------+-------------+------------+------------+------------+------------+
      * to sum up ents in different groups to complete std_turnover and app_turnover which are sums of the fields across multiple groups (check ern: 1100000004)
      * */
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
           WHEN no_ents_in_group > 1 AND record_type = 1
           THEN turnover * ($employees / group_empl_total)
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
    val t2 = "AGGREGATED"
    step1.createOrReplaceTempView(t2)

    val groupTurnoverSQL =
      s""" SELECT vat_group, SUM(app_turnover) AS grp_turnover
           FROM $t2
           WHERE record_type = 1
           GROUP BY vat_group
            """.stripMargin
    val grpTurnoversTable = "GRP_TURNOVERS"
    val grpTr = spark.sql(groupTurnoverSQL)
    grpTr.createOrReplaceTempView(grpTurnoversTable)
    val allTurnoversSQL =
      s"""
         SELECT $t2.*, $grpTurnoversTable.grp_turnover
         FROM $t2
         LEFT JOIN $grpTurnoversTable ON $t2.vat_group = $grpTurnoversTable.vat_group
       """.stripMargin
    val withGrpTr = spark.sql(allTurnoversSQL)
    val withGrpTrvr = "CALCULATED_WITH_GRP_TRVR"
    withGrpTr.createOrReplaceTempView(withGrpTrvr)

    /**
      * /**
      * * ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
      *   paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
      * * */
      * */
    val sql2 = s"SELECT ern,$employees, $jobs, SUM($contained) as $contained, SUM($apportioned) as $apportioned, SUM($standard) as $standard, grp_turnover FROM $withGrpTrvr GROUP BY ern, grp_turnover, $employees, $jobs"
    val turnovers = spark.sql(sql2)
    val t3 = "TURNOVER"
    turnovers.createOrReplaceTempView(t3)
    val sql3 =
      s"""SELECT $t3.*,
                  ((CASE
                    WHEN $t3.$standard is NULL
                    THEN 0
                    ELSE $t3.$standard
                  END) +
                 (CASE
                   WHEN $t3.$contained is NULL
                   THEN 0
                   ELSE $t3.$contained
                 END) +
                    (CASE
                      WHEN $t3.$apportioned is NULL
                      THEN 0
                      ELSE $t3.$apportioned
                    END)
                     )AS $ent
         FROM $t3""".stripMargin

    val turnoversCalculatedTable = "TURNOVERS"
    val addedEntTurnover = spark.sql(sql3)
    addedEntTurnover

  }


  def calculateTurnovers(withVatDataSQL:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ) = {

    val luTable = "LEGAL_UNITS_WITH_VAT"

    withVatDataSQL.createOrReplaceTempView(luTable)
    val appTurnover = spark.sql(
      s"""
         SELECT t1.*, t2.group_empl_total, t2.no_ents_in_group
         FROM $luTable as t1, ${selectSumEmployees(luTable)} as t2
         WHERE t1.vat_group=t2.vat_group
       """.stripMargin
    )
    appTurnover
  }

  def distinctErnsByPayeEmps(table:String) = s"(SELECT DISTINCT ern, $employees,vat_group FROM $table)"
  def selectSumEmployees(table:String) = s"""(SELECT SUM(emp_total_table.$employees) as group_empl_total, COUNT(emp_total_table.ern) as no_ents_in_group, emp_total_table.vat_group
                                                                                FROM ${distinctErnsByPayeEmps(table)} as emp_total_table
                                                                                GROUP BY emp_total_table.vat_group
                                                              )"""

  def calculateGroupTurnover(unitsDF:DataFrame, vatDF:DataFrame, payeCalculatedDF:DataFrame)(implicit spark: SparkSession ) = {
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


 /**
   * calculates paye data (non-null data quarters count, total employee count, average) for 1 paye ref
   * */

  def calculatePaye(unitsDF:DataFrame, payeDF:DataFrame)(implicit spark: SparkSession ) = {
    //printDF("unitsDF",unitsDF)
    val flatUnitDf = unitsDF.withColumn("payeref", explode_outer(unitsDF.apply("PayeRefs")))

    val luTableName = "LEGAL_UNITS"
    val payeTableName = "PAYE_DATA"

    flatUnitDf.createOrReplaceTempView(luTableName)
    payeDF.createOrReplaceTempView(payeTableName)

    spark.sql(generateCalculateAvgSQL(luTableName,payeTableName))

/*  linkedPayes.show()
    linkedPayes.printSchema()*/
  }

  def getGroupedByPayeRefs(unitsDF:DataFrame, payeDF:DataFrame, quarter:String, luTableName:String = "LEGAL_UNITS", payeDataTableName:String = "PAYE_DATA")(implicit spark: SparkSession ) = {
    //unitsDF.show()
    val flatUnitDf = unitsDF.withColumn("payeref", explode_outer(unitsDF.apply("PayeRefs")))

    flatUnitDf.createOrReplaceTempView(luTableName)
    payeDF.createOrReplaceTempView(payeDataTableName)

    val flatPayeDataSql = generateCalculateAvgSQL(luTableName,payeDataTableName)
    val sql = s"""
              SELECT SUM(AVG_CALCULATED.quarter_avg) AS $employees, CAST(SUM(AVG_CALCULATED.$quarter) AS int) AS $jobs, AVG_CALCULATED.ern
              FROM ($flatPayeDataSql) as AVG_CALCULATED
              GROUP BY AVG_CALCULATED.ern
            """.stripMargin
    spark.sql(sql)
  }

  def generateCalculateAvgSQL(luTablename:String = "LEGAL_UNITS", payeDataTableName:String = "PAYE_DATA") =
    s"""
      SELECT $luTablename.*, $payeDataTableName.mar_jobs, $payeDataTableName.june_jobs, $payeDataTableName.sept_jobs, $payeDataTableName.dec_jobs,
                CAST(
               (
                (CASE
                   WHEN $payeDataTableName.mar_jobs IS NULL
                   THEN 0
                   ELSE $payeDataTableName.mar_jobs
                 END +
                 CASE
                   WHEN $payeDataTableName.june_jobs IS NULL
                   THEN 0
                   ELSE $payeDataTableName.june_jobs
                 END +
                 CASE
                    WHEN $payeDataTableName.sept_jobs IS NULL
                    THEN 0
                    ELSE $payeDataTableName.sept_jobs
                 END +
                 CASE
                     WHEN $payeDataTableName.dec_jobs IS NULL
                     THEN 0
                     ELSE $payeDataTableName.dec_jobs
                 END)

                ) / (
                (CASE
                    WHEN $payeDataTableName.mar_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                    WHEN $payeDataTableName.june_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                    WHEN $payeDataTableName.sept_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                   WHEN $payeDataTableName.dec_jobs IS NULL
                   THEN 0
                   ELSE 1
                 END)
                ) AS int) as quarter_avg

                FROM $luTablename
                LEFT JOIN $payeDataTableName ON $luTablename.payeref=$payeDataTableName.payeref
        """.stripMargin
}

object AdminDataCalculator extends AdminDataCalculator
