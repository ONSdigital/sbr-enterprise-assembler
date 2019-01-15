package service.calculations

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object CalculateEmployment {

  @transient lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")
  /**
    * requires working_props and paye_empees recalculated
    **/
  def apply(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    log.debug("--> Start CalculateEmployment")

    df.createOrReplaceTempView("CALCULATEEMPLOYMENT")

    val sql =
      """ SELECT *,
                 CAST(
                      (CAST((CASE WHEN paye_empees is NULL THEN 0 ELSE paye_empees END) AS long) + CAST(working_props AS long))
                     AS string) AS employment
          FROM CALCULATEEMPLOYMENT
    """.stripMargin

    val res = spark.sql(sql)
    log.debug("--> End CalculateEmployment")
    res
  }
}
