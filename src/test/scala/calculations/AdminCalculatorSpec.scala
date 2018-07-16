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

  "assembler" should {
    import spark.extensions.sql._
    "calculate paye average, non-null quarter data count and eployee total using SQL api" in {

        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val unitsDF = spark.read.json(jsonFilePath).castAllToString

        val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
        val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
        val calculated: DataFrame = new AdminDataCalculator{}.calculatePayeWithSQL(unitsDF,payeDF,vatDF)
        val res =  new AdminDataCalculator{}.getGroupedByPayeRefs(unitsDF,payeDF,"dec_jobs")
        res.show()
        res.printSchema()
        spark.close()
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
