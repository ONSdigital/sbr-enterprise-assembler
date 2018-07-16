package calculations

import dao.parquet.ParquetDao
import global.AppParams
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.calculations.AdminDataCalculator
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario
import utils.{Paths, TestDataUtils}
import org.apache.spark.sql.functions.{col, explode_outer}

class AdminCalculatorSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with TestDataUtils{

   lazy val testDir = "calculations"

   val payeSchema = new StructType()
                        .add(StructField("payeref", StringType,false))
                        .add(StructField("mar_jobs", LongType,true))
                        .add(StructField("june_jobs", LongType,true))
                        .add(StructField("sept_jobs", LongType,true))
                        .add(StructField("dec_jobs", LongType,true))
                        .add(StructField("count", IntegerType,true))
                        .add(StructField("avg", IntegerType,true))

    val appConfs = AppParams((Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "ENT", "ons", "d",entHfilePath,
      "LOU", "ons", "d",louHfilePath,
      parquetPath,
      "201802",payeFilePath,
      vatFilePath,
      "local",
      "addperiod"
    )))


/*  override def beforeAll() = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()

    ParquetDao.jsonToParquet(jsonFilePath)(spark, appConfs)
    spark.stop()


  }*/

/*  override def afterAll() = {
    File(parquetPath).deleteRecursively()
    File(linkHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
    File(louHfilePath).deleteRecursively()
  }*/

/*  "AdminDataCalculator" should {
    "calculate payeRef data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val legalUnitDF = spark.read.parquet(appConfs.PATH_TO_PARQUET)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      val payesCalculated: DataFrame = AdminDataCalculator.calculatePaye(legalUnitDF,payeDF, vatDF)
      payesCalculated.printSchema()
      payesCalculated.show()
      spark.close()
    }
  }*/



/*  "assembler" should {
    "create hfiles populated with expected enterprise data using DataFrame api" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val unitsDF = spark.read.parquet(appConfs.PATH_TO_PARQUET)
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      val calculated: DataFrame = AdminDataCalculator.calculatePaye(unitsDF,payeDF, vatDF)
      calculated.show()
      calculated.printSchema()
      spark.close()
    }
  }*/

  "DataFrameHelper.generateCalculateAvgSQL" should {
    import spark.extensions.sql._
    "return sql query string which returns unitsDF with employee average calculated" in {

        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val unitsDF = spark.read.json(jsonFilePath).castAllToString

        val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
        val luTableName = "LEGAL_UNITS"
        val payeTableName = "PAYE_DATA"
        val flatUnitDf = unitsDF.withColumn("payeref", explode_outer(unitsDF.apply("PayeRefs")))
        flatUnitDf.createOrReplaceTempView(luTableName)
        payeDF.createOrReplaceTempView(payeTableName)
        val calculator = new AdminDataCalculator{}
        val sql = calculator.generateCalculateAvgSQL(luTableName, payeTableName)
        val calculated: DataFrame = spark.sql(sql)
        /*calculated.show()
        calculated.printSchema()*/
        spark.close()
      /** expected:
        * +--------------------+--------------+--------------------+----------+------------+-------+--------+---------+---------+--------+----------+
        * |        BusinessName|      PayeRefs|             VatRefs|       ern|          id|payeref|mar_jobs|june_jobs|sept_jobs|dec_jobs|quoter_avg|
        * +--------------------+--------------+--------------------+----------+------------+-------+--------+---------+---------+--------+----------+
        * |      INDUSTRIES LTD|       [1151L]|      [123123123000]|2000000011|100002826247|  1151L|       1|        2|        3|       4|         2|
        * |BLACKWELLGROUP LT...|[1152L, 1153L]|      [111222333000]|1100000003|100000246017|  1152L|       5|        6|     null|       8|         6|
        * |BLACKWELLGROUP LT...|[1152L, 1153L]|      [111222333000]|1100000003|100000246017|  1153L|       9|        1|        2|       3|         3|
        * |BLACKWELLGROUP LT...|[1154L, 1155L]|      [111222333001]|1100000003|100000827984|  1154L|       4|     null|        6|       7|         5|
        * |BLACKWELLGROUP LT...|[1154L, 1155L]|      [111222333001]|1100000003|100000827984|  1155L|       8|        9|        1|       2|         5|
        * |             IBM LTD|[1166L, 1177L]|[555666777000, 55...|1100000004|100000459235|  1166L|       1|        1|        2|       3|         1|
        * |             IBM LTD|[1166L, 1177L]|[555666777000, 55...|1100000004|100000459235|  1177L|    null|     null|     null|    null|      null|
        * |         IBM LTD - 2|[1188L, 1199L]|      [555666777002]|1100000004|100000508723|  1188L|       2|        2|        2|       2|         2|
        * |         IBM LTD - 2|[1188L, 1199L]|      [555666777002]|1100000004|100000508723|  1199L|    null|     null|     null|    null|      null|
        * |         IBM LTD - 3|[5555L, 3333L]|      [999888777000]|1100000004|100000508724|  5555L|    null|     null|     null|    null|      null|
        * |         IBM LTD - 3|[5555L, 3333L]|      [999888777000]|1100000004|100000508724|  3333L|       1|        1|        2|       3|         1|
        * |             MBI LTD|       [9876L]|      [555666777003]|2200000002|100000601835|  9876L|       6|        5|        4|       3|         4|
        * |   NEW ENTERPRISE LU|          null|      [919100010000]|9900000009|999000508999|   null|    null|     null|     null|    null|      null|
        * +--------------------+--------------+--------------------+----------+------------+-------+--------+---------+---------+--------+----------+
        * */
    }
    }


  "DataFrameHelper" should {
    import spark.extensions.sql._
    "calculate paye average, non-null quarter data count and employee total" in {

        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val unitsDF = spark.read.json(jsonFilePath).castAllToString

        val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
        val res =  new AdminDataCalculator{}.getGroupedByPayeRefs(unitsDF,payeDF,"dec_jobs")
        /*res.show()
        res.printSchema()*/
        spark.close()
      /**  Expected:
      * +--------------+---------+----------+
        |paye_employees|paye_jobs|       ern|
        +--------------+---------+----------+
        |             4|      3.0|2200000002|
        |            19|     20.0|1100000003|
        |          null|     null|9900000009|
        |             2|      4.0|2000000011|
        |             4|      8.0|1100000004|
        +--------------+---------+----------+
      * */
    }
    }



  "DataFrameHelper" should {
    import spark.extensions.sql._
    "calculate turnovers" in {

        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val unitsDF = spark.read.json(jsonFilePath).castAllToString

        val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
        val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
        val calculated: DataFrame = new AdminDataCalculator{}.calculateGroupTurnover(unitsDF,vatDF)
        calculated.show()
        calculated.printSchema()
        spark.close()
      /**expected:
        * +--------------------+--------------+--------------------+----------+------------+------------+------+
        * |        BusinessName|      PayeRefs|             VatRefs|       ern|          id|      vatref| group|
        * +--------------------+--------------+--------------------+----------+------------+------------+------+
        * |      INDUSTRIES LTD|       [1151L]|      [123123123000]|2000000011|100002826247|123123123000|123123|
        * |BLACKWELLGROUP LT...|[1152L, 1153L]|      [111222333000]|1100000003|100000246017|111222333000|111222|
        * |BLACKWELLGROUP LT...|[1154L, 1155L]|      [111222333001]|1100000003|100000827984|111222333001|111222|
        * |             IBM LTD|[1166L, 1177L]|[555666777000, 55...|1100000004|100000459235|555666777000|555666|
        * |             IBM LTD|[1166L, 1177L]|[555666777000, 55...|1100000004|100000459235|555666777001|555666|
        * |         IBM LTD - 2|[1188L, 1199L]|      [555666777002]|1100000004|100000508723|555666777002|555666|
        * |         IBM LTD - 3|[5555L, 3333L]|      [999888777000]|1100000004|100000508724|999888777000|999888|
        * |             MBI LTD|       [9876L]|      [555666777003]|2200000002|100000601835|555666777003|555666|
        * |   NEW ENTERPRISE LU|          null|      [919100010000]|9900000009|999000508999|919100010000|919100|
        * +--------------------+--------------+--------------------+----------+------------+------------+------+
        * */

    }
    }




  def printDFs(dfs:Seq[DataFrame]): Unit ={
    dfs.foreach(df => {
      df.printSchema()
      df.show()
      print("="*20+'\n')
    })
  }

}
