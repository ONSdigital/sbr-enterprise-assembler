package dao.parquet

import model.domain.Enterprise
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.reflect.io.File
/**
  *
  */
class ParquetDaoSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with TestData{

  import global.Configs._


  val jsonFilePath = "src/test/resources/data/smallWithNullValues.json"
  val linkHfilePath = "src/test/resources/data/links"
  val entHfilePath = "src/test/resources/data/enterprise"
  val parquetHfilePath = "src/test/resources/data/sample.parquet"

  override def beforeAll() = {

    conf.set("enterprise.data.timeperiod", "default")

    updateConf(Array[String](
      "LINKS", "ons", linkHfilePath,
      "ENT", "ons", entHfilePath,
      parquetHfilePath,
      "localhost",
      "2181","201802"
    ))



  }

  override def afterAll() = {
    File(parquetHfilePath).deleteRecursively()
    File(linkHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
  }


  "assembler" should {
    "create hfiles populated with expected links data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
      //implicit val ctx = spark.sparkContext

      ParquetDAO.jsonToParquet(jsonFilePath)
      ParquetDAO.parquetToHFile



      val res: Seq[Enterprise] = readEntitiesFromHFile[Enterprise](entHfilePath).collect.sortBy(_.ern).toSeq
      val expected = testEnterprises(res).sortBy(_.ern)
      res shouldBe expected


      spark.close()

    }



  }




}
