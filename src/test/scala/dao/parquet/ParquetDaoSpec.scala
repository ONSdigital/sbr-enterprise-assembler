package dao.parquet

import dao.HFileTestUtils
import global.{AppParams, Configs}
import model.domain.{Enterprise, HBaseCell, HBaseRow}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.collection.immutable
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
  val parquetHfilePath = "src/test/resources/data/sample.parquet"
  val payeFilePath = "src/test/resources/data/smallPaye.csv"

  val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "ENT", "ons", "d",entHfilePath,
      parquetHfilePath,"201802",payeFilePath
    )))


  override def beforeAll() = {

    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
    ParquetDAO.jsonToParquet(jsonFilePath)(spark,appConfs)
    spark.close()

    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

  }

  override def afterAll() = {
    File(parquetHfilePath).deleteRecursively()
  }

    override def afterEach() = {
      File(linkHfilePath).deleteRecursively()
      File(entHfilePath).deleteRecursively()
    }


  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
      ParquetDAO.parquetToHFile(spark,appConfs)

      val actual: List[Enterprise] = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList.sortBy(_.ern)
      val expected: List[Enterprise] = testEnterprises3Recs(actual).sortBy(_.ern).toList
      actual shouldBe expected


      spark.close()

    }
  }



  "assembler" should {
    "create hfiles populated with expected links data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

      ParquetDAO.parquetToHFile(spark,appConfs)

      def replaceDynamicEntIdWithStatic(entLinks:Seq[HBaseRow]) = {
        val erns = entLinks.collect{ case row if(row.key.contains("~ENT~")) => }
      }

      val actual: Seq[HBaseRow] = readEntitiesFromHFile[HBaseRow](linkHfilePath).collect.toList.sortBy(_.cells.map(_.column).mkString)//.map(entity => entity.copy(entity.key,entity.cells.sortBy(_.column)))
      val erns: Seq[(String, Int)] = actual.collect{case row if(row.cells.find(_.column=="p_ENT").isDefined) => {row.cells.collect{case HBaseCell("p_ENT",value) => value}}}.flatten.zipWithIndex
      val ernsDictionary: Seq[(String, String)] = erns.map(ernTup => {
        val (ern,index) = ernTup
        (ern,{
          "testEnterpriseId-"+({index+1}.toString*5)
        })

      })

      //replace dynamically generated erns with static in actual:
      val actualUpdated = assignStaticKeys(actual)
      val expected = testLinkRows3Recs.toSet
      actualUpdated shouldBe expected


      spark.close()
    }
  }




}
