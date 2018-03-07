package spark.calculations

import org.apache.spark.sql.functions.{array, explode_outer, udf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame

trait dataFrameHelper {

  val cols = Seq("june_jobs","sept_jobs","dec_jobs","mar_jobs")

  val avg = udf((values: Seq[Integer]) => {
    val notNullValues = values.filter(_ != null).map(_.toInt)
    notNullValues.length match {
      case 0 => None
      case s => Some(notNullValues.sum/notNullValues.length)
    }
  })

  def finalCalculations(parquetDF:DataFrame, payeDF: DataFrame) : DataFrame = {
    val latest = "dec_jobs"
    val df = flattenDataFrame(parquetDF).join(intConvert(payeDF), Seq("PayeRefs"), joinType="outer")
    val sumDf = df.groupBy("entRef").sum(latest)
    val avgDf = df.withColumn("avg", avg(array(cols.map(s => df.col(s)):_*))).select("*")
    avgDf.join(sumDf, "entRef")
  }

  private def flattenDataFrame(parquetDF:DataFrame): DataFrame = {
    parquetDF.withColumn("entRef", parquetDF.col("id")+1)
      .withColumn("VatRefs", explode_outer(parquetDF.col("VatRefs")))
      .withColumn("PayeRefs", explode_outer(parquetDF.col("PayeRefs")))
  }

  private def intConvert(payeFrame: DataFrame): DataFrame = {
    payeFrame
      .withColumn("mar_jobs", payeFrame("mar_jobs").cast(IntegerType))
      .withColumn("june_jobs", payeFrame("june_jobs").cast(IntegerType))
      .withColumn("sept_jobs", payeFrame("sept_jobs").cast(IntegerType))
      .withColumn("dec_jobs", payeFrame("dec_jobs").cast(IntegerType))
  }
}
