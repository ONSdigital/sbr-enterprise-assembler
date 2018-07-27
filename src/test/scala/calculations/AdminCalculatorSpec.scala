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
    "aggregateDF payeRef data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val legalUnitDF = spark.read.parquet(appConfs.PATH_TO_PARQUET)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      val payesCalculated: DataFrame = new AdminDataCalculator(){}.calculatePaye(legalUnitDF,payeDF)
      payesCalculated.printSchema()
      payesCalculated.show()
      spark.close()
    }
  }*/


/*  "DataFrameHelper.generateCalculateAvgSQL" should {
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
    "aggregateDF paye average, non-null quarter data count and employee total" in {

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
    }*/


  "AdminDataCalculator" should {
    import spark.extensions.sql._
    "aggregateDF turnovers test DF" in {
      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val unitsDF = spark.read.json(jsonFilePath).castAllToString
      unitsDF.show()
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      val calculator = new AdminDataCalculator(){}
      val calculatedPayeDF = calculator.getGroupedByPayeRefs(unitsDF,payeDF,"dec_jobs")
      /**
            +--------------+---------+----------+
            |paye_employees|paye_jobs|       ern|
            +--------------+---------+----------+
            |             4|        3|2200000002|
            |            19|       20|1100000003|
            |          null|     null|9900000009|
            |             2|        4|2000000011|
            |             4|        8|1100000004|
            +--------------+---------+----------+
        */

      val calculatedWithVatAndPaye = calculator.calculateGroupTurnover(unitsDF,vatDF,calculatedPayeDF)
      calculatedWithVatAndPaye.show()
      //printDF("calculatedWithVatAndPaye",calculatedWithVatAndPaye)
      val calculatedGroupEmployeeTotalAndNoEntInGroup = calculator.calculateTurnovers(calculatedWithVatAndPaye,vatDF)
      calculatedGroupEmployeeTotalAndNoEntInGroup.show()
      //printDF("calculatedGroupEmployeeTotalAndNoEntInGroup",calculatedGroupEmployeeTotalAndNoEntInGroup)
      val res = calculator.aggregateDF(calculatedGroupEmployeeTotalAndNoEntInGroup)
      res.show()
      //val step1DF: DataFrame = calculator.calculate(unitsDF,appConfs)
      /**
      +----------+---------+------------+--------+-----------+--------------+---------+
      |       ern|vat_group|      vatref|turnover|record_type|paye_employees|paye_jobs|
      +----------+---------+------------+--------+-----------+--------------+---------+
      |2000000011|   123123|123123123000|     390|          0|             2|        4|
      |1100000003|   111222|111222333000|     585|          1|            19|       20|
      |1100000003|   111222|111222333001|     590|          3|            19|       20|
      |1100000004|   555666|555666777000|    1000|          1|             4|        8|
      |1100000004|   555666|555666777001|     320|          3|             4|        8|
      |1100000004|   555666|555666777002|     340|          3|             4|        8|
      |1100000004|   999888|999888777000|     260|          0|             4|        8|
      |2200000002|   555666|555666777003|     260|          3|             5|        5|
      |9900000009|   919100|919100010000|      85|          2|          null|     null|
      +----------+---------+------------+--------+-----------+--------------+---------+
  */
/*   step1DF.show()
    val res = AdminDataCalculator.executeSql(step1DF,AdminDataCalculator.selectSumEmployees("DF"))
   res.show()
   /** +----------+--------------+---------+
       |    ern   |paye_employees|vat_group|
       +----------+--------------+---------+
       |1100000003|            19|   111222|
       |9900000009|          null|   919100|
       |1100000004|             4|   555666|
       |2000000011|             2|   123123|
       |1100000004|             4|   999888|
       |2200000002|             5|   555666|
       +----------+--------------+---------+
     */*/
   //val testDF: DataFrame = calculator.calculate(step1DF,appConfs)
   /**
    +---------+----------+------------+--------+-----------+--------------+---------+----------------+----------------+
    |vat_group|       ern|      vatref|turnover|record_type|paye_employees|paye_jobs|group_empl_total|no_ents_in_group|
    +---------+----------+------------+--------+-----------+--------------+---------+----------------+----------------+
    |   999888|1100000004|999888777000|     260|          0|             4|        8|               4|               1|
    |   111222|1100000003|111222333000|     585|          1|            19|       20|              19|               1|
    |   111222|1100000003|111222333001|     590|          3|            19|       20|              19|               1|
    |   123123|2000000011|123123123000|     390|          0|             2|        4|               2|               1|
    |   555666|1100000004|555666777000|    1000|          1|             4|        8|               9|               2|
    |   555666|1100000004|555666777001|     320|          3|             4|        8|               9|               2|
    |   555666|1100000004|555666777002|     340|          3|             4|        8|               9|               2|
    |   555666|2200000002|555666777003|     260|          3|             5|        5|               9|               2|
    |   919100|9900000009|919100010000|      85|          2|          null|     null|            null|               1|
    +---------+----------+------------+--------+-----------+--------------+---------+----------------+----------------+
   */
/*   testDF.show()
   testDF.printSchema()*/

/*   val doneDF = calculator.aggregateDF(testDF)
   doneDF.show()
   doneDF.printSchema()*/
/**
  * +----------+------------+--------------+---------+------------------+--------------------+-----------------+
  * |       ern|      vatref|paye_employees|paye_jobs|contained_turnover|apportioned_turnover|standard_turnover|
  * +----------+------------+--------------+---------+------------------+--------------------+-----------------+
  * |1100000003|111222333001|            19|       20|              null|                null|             null|
  * |2000000011|123123123000|             2|        4|              null|                null|            390.0|
  * |9900000009|919100010000|          null|     null|              null|                null|             null|
  * |1100000004|555666777001|             4|        8|              null|                null|             null|
  * |1100000004|555666777000|             4|        8|              null|                 444|             null|
  * |1100000003|111222333000|            19|       20|             585.0|                null|             null|
  * |2200000002|555666777003|             5|        5|              null|                null|             null|
  * |1100000004|555666777002|             4|        8|              null|                null|             null|
  * |1100000004|999888777000|             4|        8|              null|                null|            260.0|
  * +----------+------------+--------------+---------+------------------+--------------------+-----------------+
  * */

   spark.close()

 }}


/*  "DataFrameHelper" should {
 import spark.extensions.sql._
 "aggregateDF turnovers" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
     val unitsDF = spark.read.json(jsonFilePath).castAllToString

     val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
     val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
     val calculatedPayeDF = AdminDataCalculator.getGroupedByPayeRefs(unitsDF,payeDF,"dec_jobs")
     calculatedPayeDF.show()
     calculatedPayeDF.printSchema()
     val calculatedVat: DataFrame = new AdminDataCalculator{}.calculateGroupTurnover(unitsDF,vatDF,calculatedPayeDF)
     calculatedVat.show()
     calculatedVat.printSchema()
     spark.close()
   /**expected:
     +----------+---------+------------+--------+-----------+--------------+---------+
     |       ern|vat_group|      vatref|turnover|record_type|paye_employees|paye_jobs|
     +----------+---------+------------+--------+-----------+--------------+---------+
     |2000000011|   123123|123123123000|     390|          0|             2|        4|
     |1100000003|   111222|111222333000|     585|          1|            19|       20|
     |1100000003|   111222|111222333001|     590|          3|            19|       20|
     |1100000004|   555666|555666777000|    1000|          1|             4|        8|
     |1100000004|   555666|555666777001|     320|          3|             4|        8|
     |1100000004|   555666|555666777002|     340|          3|             4|        8|
     |1100000004|   999888|999888777000|     260|          0|             4|        8|
     |2200000002|   555666|555666777003|     260|          3|             4|        3|
     |9900000009|   919100|919100010000|      85|          2|          null|     null|
     +----------+---------+------------+--------+-----------+--------------+---------+
     */

 }
 }*/




def printDFs(dfs:Seq[DataFrame]): Unit ={
 dfs.foreach(df => {
   df.printSchema()
   df.show()
   print("="*20+'\n')
 })
}

}
