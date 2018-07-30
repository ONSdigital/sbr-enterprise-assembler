package calculations

import dao.parquet.ParquetDao
import global.AppParams
import model.domain.Calculations
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import utils.data.existing.ExistingData
import utils.data.expected.{ExpectedCalculations, ExpectedDataForAddNewPeriodScenario}
import utils.{Paths, TestDataUtils}

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


  override def beforeAll() = {
    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    ParquetDao.jsonToParquet(jsonFilePath)(spark, appConfs)
    spark.stop()

  }

  override def afterAll() = {
    File(parquetPath).deleteRecursively()
    File(linkHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
    File(louHfilePath).deleteRecursively()
  }

  "AdminDataCalculator" should {
    "aggregateDF payeRef data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val legalUnitDF = spark.read.parquet(appConfs.PATH_TO_PARQUET)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      val df: DataFrame = AdminDataCalculator.calculate(legalUnitDF,appConfs)
      val actualRows = df.collect()
      spark.close()
      val actual = actualRows.map(Calculations(_)).toSeq.sortBy(_.ern)
      val expected = expectedCalculations
      actual shouldBe expected
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
