package calculations

import dao.parquet.ParquetDao
import global.AppParams
import model.domain.Calculations
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import utils.Paths
import utils.data.existing.ExistingData
import utils.data.expected.{ExpectedCalculations, ExpectedDataForAddNewPeriodScenario}

import scala.reflect.io.File

class AdminCalculatorSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with ExpectedCalculations with RddLogging{

   lazy val testDir = "calculations"

   val payeSchema = new StructType()
                        .add(StructField("payeref", StringType,false))
                        .add(StructField("mar_jobs", LongType,true))
                        .add(StructField("june_jobs", LongType,true))
                        .add(StructField("sept_jobs", LongType,true))
                        .add(StructField("dec_jobs", LongType,true))
                        .add(StructField("count", IntegerType,true))
                        .add(StructField("avg", IntegerType,true))



  /*val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "LEU", "ons", "d", leuHfilePath,
      "ENT", "ons", "d",entHfilePath,
      "LOU", "ons", "d",louHfilePath,
      "REU", "ons", "d",ruHfilePath,
      parquetPath,
      "201804",
      payeFilePath,
      vatFilePath,
      geoFilePath,
      "local",
      "add-calculated-period"
    )))

  override def beforeAll() = {
    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    ParquetDao.jsonToParquet(jsonFilePath)(spark, appConfs)
    spark.stop()
  }


  override def afterAll() = {
    File(parquetPath).deleteRecursively()
  }



  "AdminDataCalculator.generateCalculateAvgSQL" should {
    "return sql query which results in expected set of emp_avg when executed" in {
      val expected = Array(2,6,3,5,5,1,null,2,null, null,1,5,3).map(Option(_))
      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val legalUnitDF = spark.read.parquet(appConfs.PATH_TO_PARQUET)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      import spark.implicits._
      val fullResults = generateCalculateAverageSqlTest(legalUnitDF, payeDF)
      val resDF = fullResults.map(row => Option(row.getAs[Int](10)))
      val res = resDF.rdd.collect()
      spark.close()
      res shouldBe expected
    }
  }

  "AdminDataCalculator.getGroupedByPayeRefs" should {
    "return expected set of emp_avg" in {
      val expected = Array(5,19,3,2,4).map(Option(_))
      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val legalUnitDF = spark.read.parquet(appConfs.PATH_TO_PARQUET)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      import spark.implicits._
      val resDF = AdminDataCalculator.getGroupedByPayeRefs(legalUnitDF,payeDF,"dec_jobs").map(row => Option(row.getAs[Long](0))).collect()
      spark.close()
      resDF shouldBe expected
    }
  }

  "AdminDataCalculator" should {
    "aggregateDF payeRef data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val legalUnitDF = spark.read.parquet(appConfs.PATH_TO_PARQUET)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      val df: DataFrame = AdminDataCalculator.calculate(legalUnitDF, appConfs)
      val actualRows = df.collect()
      spark.close()
      val actual = actualRows.map(Calculations(_)).toSeq.sortBy(_.ern)
      val expected = expectedCalculations
      actual shouldBe expected
    }
  }



  def generateCalculateAverageSqlTest(unitsDF:DataFrame,payeDF:DataFrame)(implicit spark: SparkSession ) = {
    val flatUnitDf = unitsDF.withColumn("payeref", explode_outer(unitsDF.apply("PayeRefs")))
    val luTableName = "LEGAL_UNITS"
    val payeDataTableName = "PAYE_DATA"
    flatUnitDf.createOrReplaceTempView(luTableName)
    payeDF.createOrReplaceTempView(payeDataTableName)
    implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    val flatPayeDataSql = AdminDataCalculator.generateCalculateAvgSQL(luTableName,payeDataTableName)
    spark.sql(flatPayeDataSql)

  }*/


}
