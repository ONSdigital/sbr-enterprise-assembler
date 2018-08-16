package utils.data.expected

import model.domain
import model.domain.Calculations

/**
  *
  */
trait ExpectedCalculations {
/**
  * +----------+-----------+---------+-------------+------------+------------+------------+------------+
  * |       ern|paye_empees|paye_jobs|cntd_turnover|app_turnover|std_turnover|grp_turnover|ent_turnover|
  * +----------+-----------+---------+-------------+------------+------------+------------+------------+
  * |1100000003|         19|       20|          585|        null|        null|        null|         585|
  * |1100000004|          4|        8|         null|         444|         260|        1000|         704|
  * |2000000011|          2|        4|         null|        null|         390|        null|         390|
  * |2200000002|          5|        5|         null|         555|        null|        1000|         555|
  * |9900000009|          3|        5|           85|        null|        null|        null|          85|
  * +----------+-----------+---------+-------------+------------+------------+------------+------------+
  *  */

  val expectedCalculations = Seq(

    Calculations("1100000003",Some(19L),Some(20),Some(585L),None,None,None,Some(585L)),
    Calculations("1100000004",Some(4L),Some(8),None,Some(444L),Some(260L),Some(1000L),Some(704L)),
    Calculations("2000000011",Some(2L),Some(4),None,None,Some(390L),None,Some(390L)),
    Calculations("2200000002",Some(5L),Some(5),None,Some(555L),None,Some(1000L),Some(555L)),
    Calculations("9900000009",Some(3L),Some(5),Some(85L),None,None,None,Some(85L))

  )

  
  
}
