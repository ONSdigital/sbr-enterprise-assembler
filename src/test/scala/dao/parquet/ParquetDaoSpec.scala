package dao.parquet

import dao.HFileTestUtils
import global.AppParams
import model.domain.{Enterprise, HFileRow}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.reflect.io.File
/**
  *
  */
class ParquetDaoSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestData with HFileTestUtils{

  import global.Configs._


  //val jsonFilePath = "src/test/resources/data/smallWithNullValues.json"
  val jsonFilePath = "src/test/resources/data/3recs.json"
  val linkHfilePath = "src/test/resources/data/links"
  val entHfilePath = "src/test/resources/data/enterprise"
  val louHfilePath = "src/test/resources/data/lou"
  val parquetHfilePath = "src/test/resources/data/sample.parquet"
  val payeFilePath = "src/test/resources/data/smallPaye.csv"
  val vatFilePath = "src/test/resources/data/smallVat.csv"

  val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "ENT", "ons", "d",entHfilePath,
      "LOU", "ons", "d",louHfilePath,
      parquetHfilePath,
      "201802",payeFilePath,
      vatFilePath,
      "local",
      "addperiod"
    )))



  override def beforeAll() = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    val confs = appConfs
    ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
    ParquetDao.parquetCreateNewToHFile(spark,appConfs)
    spark.stop()

    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

  }

  override def afterAll() = {
    File(parquetHfilePath).deleteRecursively()
    File(linkHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
    File(louHfilePath).deleteRecursively()
  }

/*    override def afterEach() = {
      File(linkHfilePath).deleteRecursively()
      File(entHfilePath).deleteRecursively()
      File(louHfilePath).deleteRecursively()
    }*/

  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", "2181")


      val actual: List[Enterprise] = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList.sortBy(_.ern)
      val expected: List[Enterprise] = testEnterprises3Recs(actual).sortBy(_.ern).toList
      actual shouldBe expected


      spark.stop()

    }
  }



  "assembler" should {
    "create hfiles populated with expected links data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
      val confs = appConfs
      //ParquetDao.parquetCreateNewToHFile(spark,appConfs)


      val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(_.cells.map(_.column).mkString)


      //replace dynamically generated erns with static in actual:
      val actualUpdated = assignStaticErns(actual)
      val expected = testLinkRows3Recs.toSet
      actualUpdated shouldBe expected


      spark.close()
    }
  }


  /*    "test content of hfile" should {
        " test" in {

          implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

          val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow]("src/main/resources/data/temp/3recsRefresh/enterprise/hfile").collect.toList.sortBy(_.cells.map(_.column).mkString)//.map(entity => entity.copy(entity.key,entity.cells.sortBy(_.column)))

          1 shouldBe 1


          spark.close()
        }
      }*/

}
