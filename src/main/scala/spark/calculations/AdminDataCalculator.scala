package spark.calculations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import spark.RddLogging
import spark.extensions.sql._


object AdminDataCalculator extends RddLogging{

  val payeSchema = new StructType()
    .add(StructField("payeref", StringType,false))
    .add(StructField("mar_jobs", LongType,true))
    .add(StructField("june_jobs", LongType,true))
    .add(StructField("sept_jobs", LongType,true))
    .add(StructField("dec_jobs", LongType,true))
    .add(StructField("total", IntegerType,true))
    .add(StructField("count", IntegerType,true))
    .add(StructField("avg", IntegerType,true))

  def caclulatePayee(payeFilePath:String, vatFilePath:String, jsonFilePath:String)(implicit spark: SparkSession ) = {

    val payeDf = spark.read.option("header", "true").csv(payeFilePath)
    val vatDf = spark.read.option("header", "true").csv(vatFilePath)

    val dataDF: DataFrame = spark.read.json(jsonFilePath).castAllToString
    dataDF.cache()

    val vatRefs: DataFrame = dataDF.withColumn("vatref", explode_outer(dataDF.apply("VatRefs"))).select("id","vatref")
    val payeeRefs: DataFrame = dataDF.withColumn("payeref", explode_outer(dataDF.apply("PayeRefs"))).select("id","payeref")

    //printDFs(Seq(payeeRefs,vatRefs))



    val calcuatedRdd: RDD[Row] = payeDf.rdd.map(getAvgEmpl)
    printRddOfRows("calcuatedRdd",calcuatedRdd)
    val payesCalculated: DataFrame = spark.createDataFrame(calcuatedRdd, payeSchema)
    payesCalculated.printSchema()
    payesCalculated.show()
    dataDF.unpersist
    payesCalculated
  }


  def getAvgEmpl(row:Row)(implicit spark: SparkSession ):Row = {

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
  }

  def doWithSqlpayeFile(payeFilePath:String, vatFilePath:String, jsonFilePath:String)(implicit spark: SparkSession ) = {

    val payeDf = spark.read.option("header", "true").csv(payeFilePath)
    val vatDf = spark.read.option("header", "true").csv(vatFilePath)

    val dataDF: DataFrame = spark.read.json(jsonFilePath).castAllToString
    dataDF.cache()
    payeDf.createOrReplaceTempView("PAYEE")
    vatDf.createOrReplaceTempView("VAT")

    val vatRefs: DataFrame = dataDF.withColumn("vatref", explode_outer(dataDF.apply("VatRefs"))).select("id","vatref")
    val payeeRefs: DataFrame = dataDF.withColumn("payeref", explode_outer(dataDF.apply("PayeRefs"))).select("id","payeref")

    payeeRefs.createOrReplaceTempView("PAYEE_REFS")
    vatRefs.createOrReplaceTempView("VAT_REFS")
    //, (PAYEE.mar_jobs + PAYEE.june_jobs + PAYEE.sept_jobs + PAYEE.dec_jobs) / ( (SELECT CASE WHEN PAYEE.mar_jobs IS NULL THEN 0 ELSE 1 END) + (SELECT CASE WHEN PAYEE.june_jobs IS NULL THEN 0 ELSE 1 END) + (SELECT CASE WHEN PAYEE.sept_jobs IS NULL THEN 0 ELSE 1 END) + (SELECT CASE WHEN PAYEE.dec_jobs IS NULL THEN 0 ELSE 1 END) ) AS AVRG
    /*      val res = spark.sql(
            """
              SELECT PAYEE_REFS.id, PAYEE_REFS.payeref, PAYEE.mar_jobs, PAYEE.june_jobs, PAYEE.sept_jobs, PAYEE.dec_jobs
              FROM PAYEE_REFS,PAYEE
              WHERE PAYEE_REFS.payeref = PAYEE.payeref
            """.stripMargin)

          res.printSchema()
          res.show()*/
    import spark.implicits._
  }
}
