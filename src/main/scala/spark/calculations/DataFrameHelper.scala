package spark.calculations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

trait DataFrameHelper {

  val cols = Seq("sum(june_jobs)","sum(sept_jobs)","sum(dec_jobs)","sum(mar_jobs)")

  val avg = udf((values: Seq[Integer]) => {
    val notNullValues = values.filter(_ != null).map(_.toInt)
    notNullValues.length match {
      case 0 => None
      case s => Some(notNullValues.sum/notNullValues.length)
    }
  })

  val prefix = udf((vatRef: Long) => if(vatRef.toString.length == 12) vatRef.toString.substring(0,9).toLong else vatRef)

  def adminCalculations(parquetDF:DataFrame, payeDF: DataFrame, vatDF: DataFrame, idColumnName: String) : DataFrame = {
    val numOfPartitions = parquetDF.rdd.getNumPartitions

    val entPaye = flattenPaye(parquetDF).join(intConvert(payeDF), Seq("payeref"), "outer")
    val employees = getEmployeeCount(entPaye, idColumnName)
    val jobs = entPaye.groupBy(idColumnName).agg(sum("dec_jobs") as "paye_jobs")

    val entVat = flattenVat(parquetDF).join(vatDF, Seq("vatref"), "outer")
    val prefixDF = entVat.withColumn("vatref9", prefix(col("vatref")))
    val startDF = prefixDF.join(broadcast(prefixDF.groupBy("vatref9").agg(countDistinct(idColumnName)as "unique").filter(col("vatref9").isNotNull)), Seq("vatref9"))

    val containedTurnover = getContainedTurnover(startDF, idColumnName)
    val standardVatTurnover = getStandardTurnover(startDF, idColumnName)
    val groupTurnover = getGroupTurnover(startDF, getEmployeeCount(entPaye, idColumnName), idColumnName)

    parquetDF
      .join(broadcast(containedTurnover),Seq(idColumnName), "outer").coalesce(numOfPartitions)
      .join(broadcast(standardVatTurnover),Seq(idColumnName), "outer").coalesce(numOfPartitions)
      .join(broadcast(getApportionedTurnover(groupTurnover, idColumnName)), Seq(idColumnName), "outer").coalesce(numOfPartitions)
      .join(broadcast(employees), idColumnName).coalesce(numOfPartitions)
      .join(broadcast(jobs), idColumnName).coalesce(numOfPartitions)
      .withColumn("total_turnover", List(coalesce(col("temp_standard_vat_turnover"), lit(0)),coalesce(col("temp_contained_rep_vat_turnover"), lit(0)),coalesce(col("apportion_turnover"), lit(0))).reduce(_+_))
      .dropDuplicates(idColumnName,"apportion_turnover")
  }

  private def flattenVat(parquetDF: DataFrame): DataFrame = {
    parquetDF
      .withColumn("vatref", explode_outer(parquetDF.apply("VatRefs")))
      .withColumnRenamed("Turnover", "TurnoverBand")
  }

  private def flattenPaye(parquetDF: DataFrame): DataFrame = {
    parquetDF.withColumn("payeref", explode_outer(parquetDF.apply("PayeRefs")))
  }

  private def intConvert(payeFrame: DataFrame): DataFrame = {
    payeFrame
      .withColumn("mar_jobs", payeFrame("mar_jobs").cast(IntegerType))
      .withColumn("june_jobs", payeFrame("june_jobs").cast(IntegerType))
      .withColumn("sept_jobs", payeFrame("sept_jobs").cast(IntegerType))
      .withColumn("dec_jobs", payeFrame("dec_jobs").cast(IntegerType))
  }

  private def getContainedTurnover(vatDF: DataFrame, idColumnName: String): DataFrame = vatDF.filter("unique == 1 and record_type == 1").groupBy(idColumnName).agg(sum("turnover") as "temp_contained_rep_vat_turnover")

  private def getStandardTurnover(vatDF: DataFrame, idColumnName: String): DataFrame = vatDF.filter("record_type == 0 or record_type == 2").groupBy(idColumnName).agg(sum("turnover") as "temp_standard_vat_turnover")

  private def getEmployeeCount(payeDF: DataFrame, idColumnName: String): DataFrame = {
    val numOfPartitions = payeDF.rdd.getNumPartitions
    val joined = payeDF
      .join(broadcast(payeDF).groupBy("PayeRefs").sum("june_jobs"),"PayeRefs").coalesce(numOfPartitions)
      .join(broadcast(payeDF).groupBy("PayeRefs").sum("sept_jobs"),"PayeRefs").coalesce(numOfPartitions)
      .join(broadcast(payeDF).groupBy("PayeRefs").sum("dec_jobs"),"PayeRefs").coalesce(numOfPartitions)
      .join(broadcast(payeDF).groupBy("PayeRefs").sum("mar_jobs"),"PayeRefs").coalesce(numOfPartitions)
      .dropDuplicates("PayeRefs")

    val avgDf = joined.withColumn("id_paye_employees", avg(array(cols.map(s => joined.apply(s)):_*)))

    payeDF
      .join(broadcast(avgDf).dropDuplicates("PayeRefs").groupBy(idColumnName).agg(sum("id_paye_employees") as "paye_employees"), Seq(idColumnName), "outer")
      .dropDuplicates(idColumnName).select(idColumnName,"paye_employees")
  }

  private def getGroupTurnover(vatDF: DataFrame, empCount: DataFrame, idColumnName: String): DataFrame = {
    val numOfPartitions = vatDF.rdd.getNumPartitions

    val nonContained = vatDF.filter("unique > 1")
      .withColumn("rep_vat", when(col("record_type").equalTo(1), 1).otherwise(0))
      .withColumn("rep_vat_turnover",col("rep_vat")*col("turnover"))
      .dropDuplicates(idColumnName,"vatref9")

    val repVatTurnover =
      nonContained
      .join(broadcast(nonContained.groupBy("vatref9").agg(sum("rep_vat_turnover") as "total_rep_vat")), Seq("vatref9"), "outer")
      .coalesce(numOfPartitions)

    val vatGroupDF =
      repVatTurnover
      .join(broadcast(repVatTurnover.groupBy("vatref9").agg(sum("rep_vat_turnover") as "vat_group_turnover")), Seq("vatref9"), "outer")
      .coalesce(numOfPartitions)

    repVatTurnover
      .join(broadcast(vatGroupDF.groupBy(idColumnName).agg(sum("vat_group_turnover") as "group_turnover")), idColumnName)
      .join(broadcast(empCount), idColumnName).coalesce(numOfPartitions)
  }

  private def getApportionedTurnover(vatDF: DataFrame, idColumnName: String): DataFrame = {
    val numOfPartitions = vatDF.rdd.getNumPartitions

    val fullCount = vatDF
      .groupBy("vatref9").agg(count("vatref9") as "full_count").toDF("countId","count")
      .join(broadcast(vatDF).filter(col("paye_employees").isNotNull).groupBy("vatref9").agg(count("vatref9") as "not_null").toDF("countId","not_null"), "countId").coalesce(numOfPartitions)
      .join(broadcast(vatDF).groupBy("vatref9").agg(sum("paye_employees") as "vat_employees").toDF("countId","vat_employees"), "countId").coalesce(numOfPartitions)
      .toDF("vatref9","full_count","not_null","vat_employees")

    val apportionDF = vatDF.join(broadcast(fullCount), "vatref9")
      .select(idColumnName,"full_count","not_null","paye_employees","vat_employees","total_rep_vat","group_turnover")
      .withColumn("boolean", when(col("full_count").equalTo(col("not_null")),1).otherwise(lit(null)))
      .withColumn("emp_prop", (col("paye_employees")/col("vat_employees"))*col("boolean"))
      .withColumn("apportion", col("emp_prop")*col("total_rep_vat"))

    apportionDF
      .select(idColumnName,"group_turnover")
      .join(broadcast(apportionDF).groupBy(idColumnName).agg(sum("apportion")as "apportion_turnover"),Seq(idColumnName), "outer").coalesce(numOfPartitions)
      .dropDuplicates(idColumnName)
  }
}