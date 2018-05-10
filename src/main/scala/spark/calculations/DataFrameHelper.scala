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

  val prefix = udf((vatRef: Long) => if(vatRef.toString.length == 12) vatRef.toString.substring(0,9).toLong else vatRef)
  val getRepVat = udf((record_type:Integer) => if (record_type == 1) 1 else 0)
  val filterNull = udf((fullCount: Long, notNull:Long) => if(fullCount == notNull) Some(1) else None)

  def adminCalculationsEnt(parquetDF:DataFrame, payeDF: DataFrame, vatDF: DataFrame, idColumnName:String = "ern") : DataFrame = {
    adminCalculations(parquetDF, payeDF, vatDF, idColumnName)
      .select(idColumnName,"paye_employees","paye_jobs","total_turnover","apportion_turnover",
        "temp_contained_rep_vat_turnover", "temp_standard_vat_turnover", "group_turnover")
  }

  def adminCalculations(parquetDF:DataFrame, payeDF: DataFrame, vatDF: DataFrame, idColumnName: String = "id") : DataFrame = {
    val partitionsCount = parquetDF.rdd.getNumPartitions
    val entPaye = flattenPaye(parquetDF).join(intConvert(payeDF), Seq("payeref"), joinType="leftOuter").coalesce(partitionsCount)
    val employees = getEmployeeCount(entPaye, idColumnName)
    val jobs = entPaye.groupBy(idColumnName).agg(sum("dec_jobs") as "paye_jobs")

    val entVat = flattenVat(parquetDF).join(vatDF, Seq("vatref"), joinType="leftOuter").coalesce(partitionsCount)
    val prefixDF = entVat.withColumn("vatref9", prefix(col("vatref")))
    val startDF = prefixDF.join(prefixDF.groupBy("vatref9").agg(countDistinct(idColumnName)as "unique").filter(col("vatref9").isNotNull), Seq("vatref9"))

    val containedTurnover = getContainedTurnover(startDF, idColumnName)
    val standardVatTurnover = getStandardTurnover(startDF, idColumnName)
    val groupTurnover = getGroupTurnover(startDF, getEmployeeCount(entPaye, idColumnName), idColumnName)

    parquetDF
      .join(containedTurnover,Seq(idColumnName), joinType="leftOuter").coalesce(partitionsCount)
      .join(standardVatTurnover,Seq(idColumnName),joinType = "leftOuter").coalesce(partitionsCount)
      .join(getApportionedTurnover(groupTurnover, idColumnName), Seq(idColumnName),joinType = "leftOuter").coalesce(partitionsCount)
      .join(groupTurnover.select(idColumnName, "group_turnover"), Seq(idColumnName), joinType = "leftOuter").coalesce(partitionsCount)
      .join(employees, idColumnName).coalesce(partitionsCount)
      .join(jobs, idColumnName).coalesce(partitionsCount)
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
    val partitionsCount = payeDF.rdd.getNumPartitions
    val joined = payeDF
      .join(payeDF.groupBy("PayeRefs").sum("june_jobs"),"PayeRefs").coalesce(partitionsCount)
      .join(payeDF.groupBy("PayeRefs").sum("sept_jobs"),"PayeRefs").coalesce(partitionsCount)
      .join(payeDF.groupBy("PayeRefs").sum("dec_jobs"),"PayeRefs").coalesce(partitionsCount)
      .join(payeDF.groupBy("PayeRefs").sum("mar_jobs"),"PayeRefs").coalesce(partitionsCount)
    joined.dropDuplicates("PayeRefs")

    val avgDf = joined.withColumn("id_paye_employees", avg(array(cols.map(s => joined.apply(s)):_*))).coalesce(partitionsCount)

    payeDF
      .join(avgDf.dropDuplicates("PayeRefs").coalesce(partitionsCount).groupBy(idColumnName)
        .agg(sum("id_paye_employees") as "paye_employees"), Seq(idColumnName), joinType = "leftOuter")
      .dropDuplicates(idColumnName).select(idColumnName,"paye_employees")

  }

  private def getGroupTurnover(vatDF: DataFrame, empCount: DataFrame, idColumnName: String): DataFrame = {
    val partitionsCount = vatDF.rdd.getNumPartitions
    val nonContained = vatDF.filter("unique > 1").withColumn("rep_vat", getRepVat(col("record_type"))).withColumn("rep_vat_turnover",col("rep_vat")*col("turnover")).dropDuplicates(idColumnName,"vatref9")
    val repVatTurnover = nonContained.join(nonContained.groupBy("vatref9").agg(sum("rep_vat_turnover") as "total_rep_vat"), Seq("vatref9")).coalesce(partitionsCount)
    val vatGroupDF = repVatTurnover.join(repVatTurnover.groupBy("vatref9").agg(sum("rep_vat_turnover") as "vat_group_turnover"), Seq("vatref9"), joinType="leftOuter").coalesce(partitionsCount)

    repVatTurnover.join(vatGroupDF.groupBy(idColumnName).agg(sum("vat_group_turnover") as "group_turnover"), idColumnName).coalesce(partitionsCount)
      .join(empCount, idColumnName).coalesce(partitionsCount)
  }

  private def getApportionedTurnover(vatDF: DataFrame, idColumnName: String): DataFrame = {
    val numOfPartitions = vatDF.rdd.getNumPartitions

    val fullCount = vatDF.groupBy("vatref9").agg(count("vatref9") as "full_count").toDF("countId","count")
      .join(vatDF.filter(col("paye_employees").isNotNull).groupBy("vatref9").agg(count("vatref9") as "not_null").toDF("countId","not_null"), "countId").coalesce(numOfPartitions)
      .join(vatDF.groupBy("vatref9").agg(sum("paye_employees") as "vat_employees").toDF("countId","vat_employees"), "countId").coalesce(numOfPartitions)
      .toDF("vatref9","full_count","not_null","vat_employees")

    val apportionDF = vatDF.join(fullCount, "vatref9")
      .withColumn("boolean", filterNull(col("full_count"),col("not_null")))
      .withColumn("emp_prop", (col("paye_employees")/col("vat_employees"))*col("boolean"))
      .withColumn("apportion", col("emp_prop")*col("total_rep_vat"))

    apportionDF
      .select(idColumnName)
      .join(apportionDF.groupBy(idColumnName).agg(sum("apportion")as "apportion_turnover"),Seq(idColumnName), joinType = "leftOuter").coalesce(numOfPartitions)
      .dropDuplicates(idColumnName)

  }

  private def getContainedTurnover(vatDF: DataFrame, idColumnName: String): DataFrame = vatDF.filter("unique == 1 and record_type == 1").groupBy(idColumnName).agg(sum("turnover") as "temp_contained_rep_vat_turnover")

  private def getStandardTurnover(vatDF: DataFrame, idColumnName: String): DataFrame = vatDF.filter("record_type == 0 or record_type == 2").groupBy(idColumnName).agg(sum("turnover") as "temp_standard_vat_turnover")

}