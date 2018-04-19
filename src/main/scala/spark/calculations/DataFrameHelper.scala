package spark.calculations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait DataFrameHelper {

  val cols = Seq("sum(june_jobs)","sum(sept_jobs)","sum(dec_jobs)","sum(mar_jobs)")

  val avg = udf((values: Seq[Integer]) => {
    val notNullValues = values.filter(_ != null).map(_.toInt)
    notNullValues.length match {
      case 0 => None
      case s => Some(notNullValues.sum/notNullValues.length)
    }
  })

  def finalCalculations(parquetDF:DataFrame, payeDF: DataFrame,idColumnName:String = "id") : DataFrame = {
    val latest = "dec_jobs"
    val partitionsCount = parquetDF.rdd.getNumPartitions

    val df = flattenDataFrame(parquetDF).join(intConvert(payeDF), Seq("payeref"), joinType="outer").coalesce(partitionsCount)
    val sumDf = df.groupBy(idColumnName).agg(sum(latest) as "paye_jobs")

    val sumQuarters = df.groupBy(idColumnName).sum("june_jobs")
      .join(df.groupBy(idColumnName).sum("sept_jobs"), idColumnName)
      .join(df.groupBy(idColumnName).sum("dec_jobs"), idColumnName)
      .join(df.groupBy(idColumnName).sum("mar_jobs"), idColumnName)
      .coalesce(partitionsCount)

    val dfQ: DataFrame = df.join(sumQuarters,idColumnName)
    val avgDf: DataFrame = dfQ.withColumn("paye_employees", avg(array(cols.map(s => dfQ.apply(s)):_*)))
    val done: Dataset[Row] = avgDf.dropDuplicates(Seq(idColumnName)).join(sumDf,idColumnName).coalesce(partitionsCount)
    //done.printSchema()
    done
  }

  private def flattenDataFrame(parquetDF:DataFrame): DataFrame = {
    parquetDF
      .withColumn("vatref", explode_outer(parquetDF.col("VatRefs")))
      .withColumn("payeref", explode_outer(parquetDF.col("PayeRefs")))
  }

  private def intConvert(payeFrame: DataFrame): DataFrame = {
    payeFrame
      .withColumn("mar_jobs", payeFrame("mar_jobs").cast(IntegerType))
      .withColumn("june_jobs", payeFrame("june_jobs").cast(IntegerType))
      .withColumn("sept_jobs", payeFrame("sept_jobs").cast(IntegerType))
      .withColumn("dec_jobs", payeFrame("dec_jobs").cast(IntegerType))
  }
}