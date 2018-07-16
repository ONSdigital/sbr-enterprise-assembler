package spark.calculations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import spark.RddLogging
import spark.extensions.sql._


trait AdminDataCalculator extends Serializable with RddLogging{


  def calculatePaye(dataDF:DataFrame, payeDF:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ):DataFrame = {

    val payeeRefs: DataFrame = dataDF.withColumn("payeref", explode_outer(dataDF.apply("PayeRefs"))).select("id","payeref")
     payeeRefs.printSchema()
    val calculatedRdd: RDD[Row] = payeDF.rdd.map(calculatePayeRef)

    spark.createDataFrame(calculatedRdd, payeCalculationSchema)


  }


  /*def getAvgEmpl(row:Row)(implicit spark: SparkSession ):Row = {

    val marchEmplCount: Option[Long] = row.getString("mar_jobs").map(_.toLong)
    val juneEmplCount: Option[Long] = row.getString("june_jobs").map(_.toLong)
    val septEmplCount: Option[Long] = row.getString("sept_jobs").map(_.toLong)
    val decEmplCount: Option[Long] = row.getString("dec_jobs").map(_.toLong)

    val count = Seq(marchEmplCount,juneEmplCount,septEmplCount,decEmplCount).filter(_.isDefined).length
    val totalEmps = if (count == 0) None
    else{
      val sum = (marchEmplCount.getOrElse(0L) + juneEmplCount.getOrElse(0L) + septEmplCount.getOrElse(0L) + decEmplCount.getOrElse(0L))
      Some(sum)
    }
    val avg =  {

      totalEmps match{
        case Some(total) => (total / count).toInt
        case _ => null
      }
    }

    new GenericRowWithSchema(Array(
      row.getString("payeref").get,
      marchEmplCount.getOrElse(null),
      juneEmplCount.getOrElse(null),
      septEmplCount.getOrElse(null),
      decEmplCount.getOrElse(null),
      totalEmps.map(_.toInt).getOrElse(null),
      if (count==0) null else count,
      avg), payeSchema)
  }*/
 /**
   * calculates paye data (non-null data quarters count, toital employee count, average) for 1 paye ref
   * */
  def calculatePayeRef(payeRow:Row)(implicit spark:SparkSession): GenericRowWithSchema = {

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

  def calculatePayeWithSQL(unitsDF:DataFrame, payeDF:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ) = {
    import spark.sqlContext.implicits._
    val res = unitsDF.map(row => {
      row.getString("id").get
    })
    val collectedRes = res.collect()
    val payes = unitsDF.select("id","VatRefs").withColumn("vatref", explode_outer(unitsDF.apply("VatRefs")))

    val flatUnitDf = unitsDF.withColumn("payeref", explode_outer(unitsDF.apply("PayeRefs")))

    unitsDF.createOrReplaceTempView("UNITS")

    val payeRefs = flatUnitDf.select("id","payeref")

    payeRefs.createOrReplaceTempView("PAYE_UNITS_LINK")
    payeDF.createOrReplaceTempView("PAYE_DATA")

    spark.sql(payeSql)
/*  linkedPayes.show()
    linkedPayes.printSchema()*/
  }

  val payeSql =
    """
      SELECT PAYE_UNITS_LINK.id, PAYE_UNITS_LINK.payeref, PAYE_DATA.mar_jobs, PAYE_DATA.june_jobs, PAYE_DATA.sept_jobs, PAYE_DATA.dec_jobs,
                (CASE
                  WHEN PAYE_DATA.mar_jobs IS NULL
                  THEN 0
                  ELSE 1
                END +
                CASE
                  WHEN PAYE_DATA.june_jobs IS NULL
                  THEN 0
                  ELSE 1
                END +
                CASE
                   WHEN PAYE_DATA.sept_jobs IS NULL
                   THEN 0
                   ELSE 1
                END +
                CASE
                    WHEN PAYE_DATA.dec_jobs IS NULL
                    THEN 0
                    ELSE 1
                END) as quarters_count,
                (CASE
                   WHEN PAYE_DATA.mar_jobs IS NULL
                   THEN 0
                   ELSE PAYE_DATA.mar_jobs
                 END +
                 CASE
                   WHEN PAYE_DATA.june_jobs IS NULL
                   THEN 0
                   ELSE PAYE_DATA.june_jobs
                 END +
                 CASE
                    WHEN PAYE_DATA.sept_jobs IS NULL
                    THEN 0
                    ELSE PAYE_DATA.sept_jobs
                 END +
                 CASE
                     WHEN PAYE_DATA.dec_jobs IS NULL
                     THEN 0
                     ELSE PAYE_DATA.dec_jobs
                 END) as total_emp_count,
                CAST(
               (
                (CASE
                   WHEN PAYE_DATA.mar_jobs IS NULL
                   THEN 0
                   ELSE PAYE_DATA.mar_jobs
                 END +
                 CASE
                   WHEN PAYE_DATA.june_jobs IS NULL
                   THEN 0
                   ELSE PAYE_DATA.june_jobs
                 END +
                 CASE
                    WHEN PAYE_DATA.sept_jobs IS NULL
                    THEN 0
                    ELSE PAYE_DATA.sept_jobs
                 END +
                 CASE
                     WHEN PAYE_DATA.dec_jobs IS NULL
                     THEN 0
                     ELSE PAYE_DATA.dec_jobs
                 END)

                ) / (
                (CASE
                    WHEN PAYE_DATA.mar_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                    WHEN PAYE_DATA.june_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                    WHEN PAYE_DATA.sept_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                   WHEN PAYE_DATA.dec_jobs IS NULL
                   THEN 0
                   ELSE 1
                 END)
                ) AS int) as quoter_avg

                FROM PAYE_UNITS_LINK,PAYE_DATA
                WHERE PAYE_UNITS_LINK.payeref = PAYE_DATA.payeref
        """.stripMargin
}

object AdminDataCalculator extends AdminDataCalculator
