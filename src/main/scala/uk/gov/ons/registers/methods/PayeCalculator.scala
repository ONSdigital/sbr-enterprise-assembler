package uk.gov.ons.registers.methods

import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}


trait PayeCalculator {

  val jobs = "paye_jobs"
  val employees = "paye_empees"

  def calculatePaye(BIDF: DataFrame, payeDF: DataFrame)(implicit activeSession: SparkSession): DataFrame = {
    val calculatedPayeEmployeesDF = getGroupedByPayeEmployees(BIDF, payeDF)
    val calculatedPayeJobsDF = getGroupedByPayeJobs(BIDF, payeDF, "dec_jobs")
    val calculatedPayeDF = calculatedPayeEmployeesDF.join(calculatedPayeJobsDF, "ern")
    //calculatedPayeDF.show

    calculatedPayeDF
  }

  def getGroupedByPayeEmployees(BIDF: DataFrame, payeDF: DataFrame, luTableName: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA")(implicit spark: SparkSession): DataFrame ={
    val flatUnitDf = BIDF.withColumn("payeref", explode_outer(BIDF.apply("PayeRefs")))
    val idDF1 = (flatUnitDf.join(payeDF, "payeref"))
    val idDF = idDF1.selectExpr("ern", "id", "cast(mar_jobs as int) mar_jobs", "cast(june_jobs as int) june_jobs", "cast(sept_jobs as int) sept_jobs", "cast(dec_jobs as int) dec_jobs")
      .groupBy("id").agg(sum("mar_jobs") as "mar_jobs", sum("june_jobs") as "june_jobs", sum("sept_jobs") as "sept_jobs", sum("dec_jobs") as "dec_jobs")

    //missingPayeRefsThrow(flatUnitDf,idDF1)

    idDF.createOrReplaceTempView(payeDataTableName)
    flatUnitDf.createOrReplaceTempView(luTableName)

    val flatPayeDataSumSql = generateCalculateSumSQL(luTableName, payeDataTableName)
    val flatPayeDataCountSql = generateCalculateCountSQL(luTableName, payeDataTableName)

    val sqlSum =  s"""
              SELECT (SUM(AVG_CALCULATED.quarter_sum)) AS sums, AVG_CALCULATED.id, AVG_CALCULATED.ern
              FROM ($flatPayeDataSumSql) as AVG_CALCULATED
              GROUP BY AVG_CALCULATED.ern, AVG_CALCULATED.id
            """.stripMargin
    val sqlCount = s"""
              SELECT (SUM(AVG_CALCULATED.quarter_count)) AS counts, AVG_CALCULATED.id
              FROM ($flatPayeDataCountSql) as AVG_CALCULATED
              GROUP BY AVG_CALCULATED.id
            """.stripMargin
    val Sum = spark.sql(sqlSum)
    val Count = spark.sql(sqlCount)
    val aggDF = Sum.join(Count, "id")

    val ungroupedDF = aggDF.withColumn(employees, aggDF.col("sums") / aggDF.col("counts"))//.selectExpr("ern", s"cast($employees as int) $employees")
    val groupedDF = ungroupedDF.groupBy("ern").agg(sum(employees) as employees).selectExpr(s"cast($employees as int) $employees", "ern")

    groupedDF
  }

  def getGroupedByPayeJobs(BIDF: DataFrame, payeDF: DataFrame, quarter: String,luTableName: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA")(implicit spark: SparkSession): DataFrame ={
    val flatUnitDf = BIDF.withColumn("payeref", explode_outer(BIDF.apply("PayeRefs")))
    val idDF = (payeDF.join(flatUnitDf, "payeref")).selectExpr("ern", s"cast($quarter as int) $quarter").groupBy("ern").agg(sum(quarter) as jobs)
    idDF
  }

  def generateCalculateSumSQL(luTablename: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA") =
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
                ) AS int) as quarter_sum
                FROM $luTablename
                LEFT JOIN $payeDataTableName ON $luTablename.id=$payeDataTableName.id
        """.stripMargin

  def generateCalculateCountSQL(luTablename: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA") =
    s"""
      SELECT $luTablename.*, $payeDataTableName.mar_jobs, $payeDataTableName.june_jobs, $payeDataTableName.sept_jobs, $payeDataTableName.dec_jobs,
                CAST(
               (
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
                ) AS int) as quarter_count
                FROM $luTablename
                LEFT JOIN $payeDataTableName ON $luTablename.id=$payeDataTableName.id
        """.stripMargin

  def missingPayeRefsThrow(BIDF: DataFrame, PayeDF: DataFrame): Unit = {
    val BList = BIDF.select("payeref").collect.toList
    val PList = PayeDF.select("payeref").collect.toList
    val diff = (BList.diff(BList.intersect(PList))).mkString(" ")
    assert(BList.forall(PList.contains), s"Expected exception to be thrown as the PayeRef(s) $diff don't exist in the Paye input")
  }
  //    /**
  //      * calculates paye data (non-null data quarters count, total employee count, average) for 1 paye ref
  //      * */
  //
  //    def calculatePaye(unitsDF:DataFrame, payeDF:DataFrame)(implicit spark: SparkSession ) = {
  //      //printDF("unitsDF",unitsDF)
  //      val flatUnitDf = unitsDF.withColumn("payeref", explode_outer(unitsDF.apply("PayeRefs")))
  //
  //      val luTableName = "LEGAL_UNITS"
  //      val payeTableName = "PAYE_DATA"
  //
  //      flatUnitDf.createOrReplaceTempView(luTableName)
  //      payeDF.createOrReplaceTempView(payeTableName)
  //
  //      spark.sql(generateCalculateAvgSQL(luTableName,payeTableName))
  //
  //      /*  linkedPayes.show()
  //          linkedPayes.printSchema()*/
  //    }

}
