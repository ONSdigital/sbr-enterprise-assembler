package spark.calculations

import model.domain.LegalUnit
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.RddLogging
import spark.extensions.sql._


trait AdminDataCalculator extends Serializable with RddLogging{

  def distinctErnsByPayeEmps(table:String) = s"(SELECT DISTINCT ern, paye_employees,vat_group FROM $table)"
  def selectSumEmployees(table:String) = s"""(SELECT SUM(emp_total_table.paye_employees) as group_empl_total, emp_total_table.vat_group
                                                                                FROM ${distinctErnsByPayeEmps(table)} as emp_total_table
                                                                                GROUP BY emp_total_table.vat_group
                                                              )"""

  def executeSql(df:DataFrame,sql:String)(implicit spark: SparkSession ) = {
    df.createOrReplaceTempView("DF")
    val query = sql.replace("£table","DF")
    spark.sql(query)
  }

  def generateWithVatSQL(luTable:String, payeTable:String, vatTable:String) = {
    s"""SELECT $luTable.vat_group, $luTable.ern,  $luTable.vatref, $vatTable.turnover, $vatTable.record_type, $payeTable.paye_employees, $payeTable.paye_jobs
         FROM $luTable, $vatTable, $payeTable
         WHERE $luTable.vatref=$vatTable.vatref AND $payeTable.ern=$luTable.ern""".stripMargin
  }

  def calculateTurnoverTest(withVatDataSQL:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ) = {

    val luTable = "LEGAL_UNITS_WITH_VAT"

    withVatDataSQL.createOrReplaceTempView(luTable)
    val appTurnover = spark.sql(
      s"""
         SELECT t1.*, t2.group_empl_total
         FROM $luTable as t1, ${selectSumEmployees(luTable)} as t2
         WHERE t1.vat_group=t2.vat_group
       """.stripMargin
    )
    appTurnover
  }

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

  def generateCalculateWeightsSQL(vatGroup:String,tableName:String = "LEGAL_UNITS") =
    s"""SELECT SUM(quarter_avg), vat_group
       GROUP BY vat_group
    """.stripMargin

 /**
   * calculates paye data (non-null data quarters count, toital employee count, average) for 1 paye ref
   * */
  def calculatePayeRef(payeRow:Row)(implicit spark:SparkSession): GenericRowWithSchema = {
    import spark.implicits._
    val payeRef = payeRow.getString("payeref").get

    val q1 = payeRow.getString("mar_jobs").map(_.toInt)
    val q2 = payeRow.getString("june_jobs").map(_.toInt)
    val q3 = payeRow.getString("sept_jobs").map(_.toInt)
    val q4 = payeRow.getString("dec_jobs").map(_.toInt)

    val all_qs = Seq(q1,q2,q3,q4)

    val (count,sum): (Int, Int) = all_qs.foldLeft((0,0))((count_and_sum, q) => {
      val (quarters_count,emplTotal) = count_and_sum
      q match{
        case Some(currentQuarterEmplCount) => {
          if(quarters_count==null) (quarters_count + 1,emplTotal + currentQuarterEmplCount)
          else count_and_sum
        }
        case _ => count_and_sum
      }
    })

    val avg = if(count.isValidInt && count!=0) (sum / count) else null

    new GenericRowWithSchema(payeRef+:all_qs.toArray.map(_.getOrElse(null)):+count:+sum:+avg
    , payeCalculationSchema)
  }

  def calculatePaye(unitsDF:DataFrame, payeDF:DataFrame)(implicit spark: SparkSession ) = {
    import spark.sqlContext.implicits._
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

    val flatUnitDf = unitsDF.withColumn("payeref", explode_outer(unitsDF.apply("PayeRefs")))

    flatUnitDf.createOrReplaceTempView(luTableName)
    payeDF.createOrReplaceTempView(payeDataTableName)

    val flatPayeDataSql = generateCalculateAvgSQL(luTableName,payeDataTableName)
    val sql = s"""
              SELECT SUM(AVG_CALCULATED.quarter_avg) AS paye_employees, CAST(SUM(AVG_CALCULATED.$quarter) AS int) AS paye_jobs, AVG_CALCULATED.ern
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
