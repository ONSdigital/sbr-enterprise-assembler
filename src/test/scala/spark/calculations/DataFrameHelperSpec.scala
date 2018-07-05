package spark.calculations


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

  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val payesCalculated: DataFrame = AdminDataCalculator.caclulatePayee(payeFilePath,vatFilePath,jsonFilePath)
      payesCalculated.printSchema()
      payesCalculated.show()
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
