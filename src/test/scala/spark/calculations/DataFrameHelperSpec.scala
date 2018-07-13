package spark.calculations


import dao.parquet.ParquetDao
import global.AppParams
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.sql._
import test.Paths
import test.data.existing.ExistingData
import test.data.expected.ExpectedDataForAddNewPeriodScenario
import test.utils.TestDataUtils

import scala.reflect.io.File

class DataFrameHelperSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with TestDataUtils{

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
/*
  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val payesCalculated: DataFrame = AdminDataCalculator.caclulatePayee(payeFilePath,vatFilePath,jsonFilePath)
      payesCalculated.printSchema()
      payesCalculated.show()
      spark.close()
    }
    }*/


  "assembler" should {
    "create hfiles populated with expected enterprise data using DataFrame api" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val unitsDF = spark.read.parquet(appConfs.PATH_TO_PARQUET)
      val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
      val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
      import spark.sqlContext.implicits._
      val calculated: DataFrame = AdminDataCalculator.calculatePaye(unitsDF,payeDF, vatDF)
      calculated.show()
      calculated.printSchema()
      spark.close()
    }
  }

/*
  "assembler" should {
    "create hfiles populated with expected enterprise data using SQL api" in {

        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val unitsDF = spark.read.parquet(appConfs.PATH_TO_PARQUET).castAllToString

        val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
        val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)
        val calculated: DataFrame = new AdminDataCalculator{}.calculatePayeWithSQL(unitsDF,payeDF,vatDF)
        calculated.show()
        calculated.printSchema()
        spark.close()
    }
    }
*/




  def printDFs(dfs:Seq[DataFrame]): Unit ={
    dfs.foreach(df => {
      df.printSchema()
      df.show()
      print("="*20+'\n')
    })
  }

}
