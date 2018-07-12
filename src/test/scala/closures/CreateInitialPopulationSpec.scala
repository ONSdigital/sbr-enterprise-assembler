package closures

import closures.mocks.MockClosures
import dao.parquet.ParquetDao
import global.AppParams
import model.domain.{Enterprise, HFileRow, LocalUnit}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._
import test.Paths
import test.data.expected.ExpectedDataForCreatePopulationScenario
import test.utils.TestDataUtils

import scala.reflect.io.File
/**
  *
  */
class CreateInitialPopulationSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with TestDataUtils with ExpectedDataForCreatePopulationScenario{

  lazy val testDir = "create"

  object MockCreateNewPopulationClosure extends CreateClosures with MockClosures

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

    val confs = appConfs

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()

    ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
    MockCreateNewPopulationClosure.parquetCreateNewToHFile(spark,appConfs)
    spark.stop()


  }

  override def afterAll() = {
   File(parquetPath).deleteRecursively()
   File(linkHfilePath).deleteRecursively()
   File(entHfilePath).deleteRecursively()
   File(louHfilePath).deleteRecursively()
 }

 "assembler" should {
   "create hfiles populated with expected enterprise data" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()


     val actualHFileRows: List[HFileRow] = readEntitiesFromHFile[HFileRow](entHfilePath).collect.toList
     val actual = actualHFileRows.map(Enterprise(_)).sortBy(_.businessName)


     actual shouldBe expectedEnts


     spark.stop()

   }
 }


"assembler" should {
   "create hfiles populated with expected links data" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
     val confs = appConfs
     val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(_.key)
     val expected = expectedLinks.sortBy(_.key)
     actual shouldBe expected


     spark.close()
   }
 }



  "assembler" should {
   "create hfiles populated with expected local unit data" in {

         implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

         val actual: Seq[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList.sortBy(_.name)
         val expected = expectedLous.sortBy(_.name)
         actual shouldBe expected


         spark.close()
       }
     }
}