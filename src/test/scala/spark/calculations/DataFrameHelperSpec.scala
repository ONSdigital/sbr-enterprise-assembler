package spark.calculations


import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.sql._
import test.Paths
import test.data.existing.ExistingData
import test.data.expected.ExpectedDataForAddNewPeriodScenario
import test.utils.TestDataUtils

class DataFrameHelperSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with TestDataUtils{

  lazy val testDir = "calculations"

  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()

      val payeDf = spark.read.option("header", "true").csv(payeFilePath)
      val vatDf = spark.read.option("header", "true").csv(vatFilePath)

      val dataDF: DataFrame = spark.read.json(jsonFilePath).castAllToString
      dataDF.cache()

        val payeeRefs: DataFrame = dataDF.withColumn("vatref", explode_outer(dataDF.apply("VatRefs"))).select("id","vatref")
        val vatRefs: DataFrame = dataDF.withColumn("payeeref", explode_outer(dataDF.apply("PayeRefs"))).select("id","payeeref")

      payeeRefs.printSchema()
      payeeRefs.show()

      print("="*10+'\n')

      vatRefs.printSchema()
      vatRefs.show()

      dataDF.unpersist
    }
    }

}
