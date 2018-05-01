package spark.calculations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import spark.RddLogging

trait DataFrameHelper/* extends RddLogging*/{

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
    val numOfPartitions = parquetDF.rdd.getNumPartitions

    val df = flattenDataFrame(parquetDF).join(intConvert(payeDF), Seq("payeref"), joinType="leftOuter").coalesce(numOfPartitions)
    // printDF("df joining paye and new period data",df)
    val sumDf = df.groupBy(idColumnName).agg(sum(latest) as "paye_jobs")

    val avgDf = getEmployeeCount(df, idColumnName)
    // printDF("avgDf",avgDf)

    val done: Dataset[Row] = avgDf.dropDuplicates(Seq(idColumnName)).join(sumDf,idColumnName).coalesce(numOfPartitions)
    //// printRddOfRows("done",done)
    done
  }

  def finalCalculationsEnt(parquetDF:DataFrame, payeDF: DataFrame,idColumnName:String = "ern") : DataFrame = {
    val latest = "dec_jobs"
    val partitionsCount = parquetDF.rdd.getNumPartitions

    val df = flattenDataFrame(parquetDF).join(intConvert(payeDF), Seq("payeref"), joinType="leftOuter").coalesce(partitionsCount)
    // printDF("df joining paye and new period data",df)
    val sumDf = df.groupBy(idColumnName).agg(sum(latest) as "paye_jobs")

    val avgDf = getEmployeeCount(df,idColumnName)

    val done: Dataset[Row] = avgDf.dropDuplicates(Seq(idColumnName)).join(sumDf,idColumnName).select(idColumnName,"paye_employees","paye_jobs")
    // print(s"finalCalculationsEnt.  NUM OF PARTITIONS WAS:$partitionsCount, after join: "+done.rdd.getNumPartitions)
    done//.coalesce(partitionsCount)
  }

  private def flattenDataFrame(parquetDF:DataFrame): DataFrame = {
    val res = parquetDF
      .withColumn("vatref", explode_outer(parquetDF.col("VatRefs")))
      .withColumn("payeref", explode_outer(parquetDF.col("PayeRefs")))

    // printDF("flattened DataFrame", res)
    res
  }

  private def intConvert(payeFrame: DataFrame): DataFrame = {
    val res = payeFrame
      .withColumn("mar_jobs", payeFrame("mar_jobs").cast(IntegerType))
      .withColumn("june_jobs", payeFrame("june_jobs").cast(IntegerType))
      .withColumn("sept_jobs", payeFrame("sept_jobs").cast(IntegerType))
      .withColumn("dec_jobs", payeFrame("dec_jobs").cast(IntegerType))
    // printDF("int Converted DataFrame", res)
    // print(s"finalCalculationsEnt.  NUM OF PARTITIONS WAS:${payeFrame.rdd.getNumPartitions}, after join: ${res.rdd.getNumPartitions}")
    res
  }

  private def getEmployeeCount(payeDF: DataFrame, idColumnName: String): DataFrame = {
    val numOfPartitions = payeDF.rdd.getNumPartitions
    val joined = payeDF
      .join(payeDF.groupBy("PayeRefs").sum("june_jobs"),"PayeRefs").coalesce(numOfPartitions)
      .join(payeDF.groupBy("PayeRefs").sum("sept_jobs"),"PayeRefs").coalesce(numOfPartitions)
      .join(payeDF.groupBy("PayeRefs").sum("dec_jobs"),"PayeRefs").coalesce(numOfPartitions)
      .join(payeDF.groupBy("PayeRefs").sum("mar_jobs"),"PayeRefs").coalesce(numOfPartitions)
    joined.dropDuplicates("PayeRefs")

    val avgDf = joined.withColumn("id_paye_employees", avg(array(cols.map(s => joined.apply(s)):_*))).coalesce(numOfPartitions)
    payeDF.join(avgDf.dropDuplicates("PayeRefs").groupBy(idColumnName).agg(sum("id_paye_employees") as "paye_employees"), Seq(idColumnName), joinType = "leftOuter").coalesce(numOfPartitions)
  }

}