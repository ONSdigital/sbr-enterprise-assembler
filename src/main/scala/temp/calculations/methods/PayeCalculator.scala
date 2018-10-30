package temp.calculations.methods

import org.apache.spark.sql.functions.{explode_outer, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.extensions.sql._



trait PayeCalculator {
  import CommonFrameDataFields._

  val jobs = "paye_jobs"
  val employees = "paye_empees"

  def calculatePAYE(BIDF: DataFrame, payeDF: DataFrame)(implicit activeSession: SparkSession): DataFrame = {
    val calculatedPayeEmployeesDF = getGroupedByPayeEmployees(BIDF, payeDF)
    val calculatedPayeJobsDF = getGroupedByPayeJobs(BIDF, payeDF, dec_jobs)
    val calculatedPayeDF = calculatedPayeEmployeesDF.join(calculatedPayeJobsDF, "ern")
    //calculatedPayeDF.show

    calculatedPayeDF
  }

  def getGroupedByPayeEmployees(BIDF: DataFrame, payeDF: DataFrame, luTableName: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA")(implicit spark: SparkSession): DataFrame ={
    val flatUnitDf = BIDF.filter(_.getStringSeq(PayeRefs).isDefined).withColumn(payeRefs, explode_outer(BIDF.apply(PayeRefs)))
    val idDF1 = (flatUnitDf.join(payeDF, payeRefs))
    val idDF = idDF1.selectExpr(ern, id, s"cast($mar_jobs as int) $mar_jobs", s"cast($june_jobs as int) $june_jobs", s"cast($sept_jobs as int) $sept_jobs", s"cast($dec_jobs as int) $dec_jobs")
      .groupBy(id).agg(sum(mar_jobs) as mar_jobs, sum(june_jobs) as june_jobs, sum(sept_jobs) as sept_jobs, sum(dec_jobs) as dec_jobs)

    missingPayeRefsThrow(flatUnitDf,idDF1)

    idDF.createOrReplaceTempView(payeDataTableName)
    flatUnitDf.createOrReplaceTempView(luTableName)

    val flatPayeDataSumSql = generateCalculateSumSQL(luTableName, payeDataTableName)
    val flatPayeDataCountSql = generateCalculateCountSQL(luTableName, payeDataTableName)

    val sqlSum =  s"""
              SELECT (SUM(AVG_CALCULATED.quarter_sum)) AS sums, AVG_CALCULATED.$id, AVG_CALCULATED.ern
              FROM ($flatPayeDataSumSql) as AVG_CALCULATED
              GROUP BY AVG_CALCULATED.ern, AVG_CALCULATED.$id
            """.stripMargin
    val sqlCount = s"""
              SELECT (SUM(AVG_CALCULATED.quarter_count)) AS counts, AVG_CALCULATED.$id
              FROM ($flatPayeDataCountSql) as AVG_CALCULATED
              GROUP BY AVG_CALCULATED.$id
            """.stripMargin
    val Sum = spark.sql(sqlSum)
    val Count = spark.sql(sqlCount)
    val aggDF = Sum.join(Count, id)

    val ungroupedDF = aggDF.withColumn(employees, aggDF.col("sums") / aggDF.col("counts"))//.selectExpr("ern", s"cast($employees as int) $employees")
    val groupedDF = ungroupedDF.groupBy(ern).agg(sum(employees) as employees).selectExpr(s"cast($employees as int) $employees", ern)

    groupedDF
  }

  def getGroupedByPayeJobs(BIDF: DataFrame, payeDF: DataFrame, quarter: String,luTableName: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA")(implicit spark: SparkSession): DataFrame ={
    val flatUnitDf = BIDF.withColumn(payeRefs, explode_outer(BIDF.apply(PayeRefs)))
    val idDF = (payeDF.join(flatUnitDf, payeRefs)).selectExpr(ern, s"cast($quarter as int) $quarter").groupBy(ern).agg(sum(quarter) as jobs)
    idDF
  }

  def generateCalculateSumSQL(luTablename: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA") =
    s"""
      SELECT $luTablename.*, $payeDataTableName.$mar_jobs, $payeDataTableName.$june_jobs, $payeDataTableName.$sept_jobs, $payeDataTableName.$dec_jobs,
                CAST(
               ((${sumSqlText(payeDataTableName, mar_jobs)} +
                 ${sumSqlText(payeDataTableName, june_jobs)} +
                 ${sumSqlText(payeDataTableName, sept_jobs)} +
                 ${sumSqlText(payeDataTableName, dec_jobs)})
                ) AS int) as quarter_sum
                FROM $luTablename
                LEFT JOIN $payeDataTableName ON $luTablename.$id=$payeDataTableName.$id
        """.stripMargin

  def generateCalculateCountSQL(luTablename: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA") =
    s"""
      SELECT $luTablename.*, $payeDataTableName.$mar_jobs, $payeDataTableName.$june_jobs, $payeDataTableName.$sept_jobs, $payeDataTableName.$dec_jobs,
                CAST(
               ((${countSqlText(payeDataTableName, mar_jobs)} +
                 ${countSqlText(payeDataTableName, june_jobs)} +
                 ${countSqlText(payeDataTableName, sept_jobs)} +
                 ${countSqlText(payeDataTableName, dec_jobs)})
                ) AS int) as quarter_count
                FROM $luTablename
                LEFT JOIN $payeDataTableName ON $luTablename.$id=$payeDataTableName.$id
        """.stripMargin

  def sumSqlText(tableName: String, date: String) =
    s"CASE WHEN $tableName.$date IS NULL THEN 0 ELSE $tableName.$date END"

  def countSqlText(tableName: String, date: String) =
    s"CASE WHEN $tableName.$date IS NULL THEN 0 ELSE 1 END"

  def missingPayeRefsThrow(BIDF: DataFrame, PayeDF: DataFrame): Unit = {
    BIDF.cache()
    PayeDF.cache()
    val BList = BIDF.filter(! _.isNull(payeRefs))
    val PList = PayeDF.select(payeRefs)
    val diff = BList.join(PList, Seq(payeRefs), "left_anti")
    val count = diff.count()
    assert(count==0, s"Expected exception to be thrown as the PayeRef(s) as $count payes refs don't exist in the Paye input")
    BIDF.unpersist()
    PayeDF.unpersist()
  }

}

