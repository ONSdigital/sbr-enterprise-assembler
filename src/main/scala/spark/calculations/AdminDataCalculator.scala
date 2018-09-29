package spark.calculations

import global.AppParams
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import spark.RddLogging
import org.apache.spark.sql.functions.lit
import uk.gov.ons.registers.methods


trait AdminDataCalculator extends Serializable with RddLogging{
//paye_empees, $jobs, app_turnover, $ent, cntd_turnover, std_turnover, grp_turnover
  val jobs = "paye_jobs"
  val employees = "paye_empees"
  val apportioned = "app_turnover"
  val contained = "cntd_turnover"
  val ent = "ent_turnover"
  val standard = "std_turnover"
  val group_turnover = "grp_turnover"

  def calculate(unitsDF:DataFrame,appConfs:AppParams)(implicit spark: SparkSession ) = {

    val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
    val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)

    val calculatedPayeDF = getGroupedByPayeRefs(unitsDF,payeDF,"dec_jobs")

    val calculatedWithVatAndPaye = joinWithVatAndPayeData(unitsDF,vatDF,calculatedPayeDF)

    val calculatedTurnovers = calculateTurnovers(calculatedWithVatAndPaye)

    val aggregatedDF = aggregateDF(calculatedTurnovers)

    //val withWorkingPropsDF = calculateWorkingProps(aggregatedDF)

    //val withEmploymentDF = calculateEmployments(aggregatedDF)

    //withEmploymentDF

    aggregatedDF

  }

  //def calculateWorkingProps(df:DataFrame) = df.withColumn("working_props",lit("0"))
  //def calculateEmployments(df:DataFrame) = df.withColumn("employment",lit("0"))


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

  def readSql(df:DataFrame,sql:(String) => String)(implicit spark:SparkSession) = {
    val tablename = "DFTEMP"
    df.createOrReplaceTempView(tablename)
    val query = sql(tablename)
    val res = spark.sql(query)
    res
  }

}

object AdminDataCalculator extends AdminDataCalculator
