package spark.calculations

import model.domain.LegalUnit
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.RddLogging
import spark.extensions.sql._


trait AdminDataCalculator extends Serializable with RddLogging{


/*  def calculatePaye(dataDF:DataFrame, payeDF:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ):DataFrame = {

    import spark.implicits._

    val payeeRefs: DataFrame = dataDF.withColumn("payeref", explode_outer(dataDF.apply("PayeRefs"))).select("id","payeref")
    payeeRefs.printSchema()

    payeDF.map(calculatePayeRef).toDF()
/*    val calculatedRdd: RDD[Row] = payeDF.rdd.map(calculatePayeRef)

    spark.createDataFrame(calculatedRdd, payeCalculationSchema)*/


  }*/
/*
  def toCalculatedPayes(lus:DataFrame, payeDF:DataFrame)(implicit spark: SparkSession ) = {
    val payeRefs: DataFrame = lus.withColumn("payeref", explode_outer(lus.apply("PayeRefs"))).select("id","ern", "payeref")
    payeRefs
    val calculatedPayeRefs = payeDF.map(calculatePayeRef)
    calculatedPayeRefs

  }*/

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

  def calculatePayeWithSQL(unitsDF:DataFrame, payeDF:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ) = {
    import spark.sqlContext.implicits._
    printDF("unitsDF",unitsDF)
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
              SELECT SUM(AVG_CALCULATED.quoter_avg) AS paye_employees, SUM(AVG_CALCULATED.$quarter) AS paye_jobs, AVG_CALCULATED.ern
              FROM ($flatPayeDataSql) as AVG_CALCULATED
              GROUP BY AVG_CALCULATED.ern
            """.stripMargin
    spark.sql(sql)
  }

  def generateCalculateAvgSQL(luTablename:String, payeDataTableName:String) =
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
                ) AS int) as quoter_avg

                FROM $luTablename
                LEFT JOIN $payeDataTableName ON $luTablename.payeref=$payeDataTableName.payeref
        """.stripMargin
}

object AdminDataCalculator extends AdminDataCalculator
