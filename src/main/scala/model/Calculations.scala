package model

import dao.DaoUtils._
import org.apache.spark.sql.Row

/**
  * |-- ern: string (nullable = true)
  * |-- paye_empees: long (nullable = true)
  * |-- paye_jobs: integer (nullable = true)
  * |-- cntd_turnover: long (nullable = true)
  * |-- app_turnover: long (nullable = true)
  * |-- std_turnover: long (nullable = true)
  * |-- grp_turnover: long (nullable = true)
  * |-- ent_turnover: long (nullable = true)
  */
//ern|paye_empees|paye_jobs|cntd_turnover|app_turnover|std_turnover|grp_turnover|ent_turnover
case class Calculations(
                         ern: String, empl: Option[Long], jobs: Option[Int], contained: Option[Long],
                         apportioned: Option[Long], standard: Option[Long],
                         group: Option[Long], ent: Option[Long]
                       )

object Calculations {
  def apply(row: Row): Calculations = {
    val appd = row.getAs[Long]("app_turnover")
    new Calculations(
      row.getAs[String]("ern"),
      row.getOption[Long]("paye_empees"),
      row.getOption[Int]("paye_jobs"),
      row.getOption[Long]("cntd_turnover"),
      row.getOption[Long]("app_turnover"),
      row.getOption[Long]("std_turnover"),
      row.getOption[Long]("grp_turnover"),
      row.getOption[Long]("ent_turnover")
    )
  }

}
