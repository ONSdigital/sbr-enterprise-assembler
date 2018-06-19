package dao.parquet

import dao.HFileTestUtils
import global.AppParams
import model.domain.{Enterprise, HFileRow, LocalUnit}
import model.hfile
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.reflect.io.File
/**
  *
  */
class CreateNewPopulationFromParquetSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestData with HFileTestUtils{

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

    val confs = appConfs
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()

    ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
    ParquetDao.parquetCreateNewToHFile(spark,appConfs)
    spark.stop()


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


     //HFileRow-s need to be flatMapped to HFileCell-s to avoid ordering mismatch on HFileRow.cells:
     val actualUpdated = assignStaticLinkIds(actual).flatMap(row => row.toHFileCells(confs.HBASE_ENTERPRISE_COLUMN_FAMILY))
     val expected = testLinkRows3Recs.flatMap(row => row.toHFileCells(confs.HBASE_ENTERPRISE_COLUMN_FAMILY)).toSet
     actualUpdated shouldBe expected


     spark.close()
   }
 }


 "assembler" should {
   "create hfiles populated with expected local unit data" in {

         implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

         val actual: Seq[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList.sortBy(_.name)
         val actualUpdated = assignStaticIds(actual)
         val expected = testLocalUnitsRows3Recs
         actualUpdated shouldBe expected


         spark.close()
       }
     }

}
