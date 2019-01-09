package service.calculations

import org.apache.spark.sql.{DataFrame, SparkSession}

object CalculateEmployment {

  /**
    * requires working_props and paye_empees recalculated
    **/
  def apply(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    df.createOrReplaceTempView("CALCULATEEMPLOYMENT")

    val sql =
      """ SELECT *,
                 CAST(
                      (CAST((CASE WHEN paye_empees is NULL THEN 0 ELSE paye_empees END) AS long) + CAST(working_props AS long))
                     AS string) AS employment
          FROM CALCULATEEMPLOYMENT
    """.stripMargin

    spark.sql(sql)
  }
}
