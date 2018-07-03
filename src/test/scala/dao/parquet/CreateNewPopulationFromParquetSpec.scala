package dao.parquet

import dao.HFileTestUtils
import global.AppParams
import model.domain.{Enterprise, HFileRow, LocalUnit}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.reflect.io.File
/**
  *
  */
class CreateNewPopulationFromParquetSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with TestDataUtils with ExistingData{

  import global.Configs._

  lazy val testDir = "create"


  val appConfs = AppParams(
    (Array[String](
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

    val confs = appConfs
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()

    //ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
    ParquetDao.parquetCreateNewToHFile(spark,appConfs)
    spark.stop()


  }*/

/*  override def afterAll() = {
   File(parquetPath).deleteRecursively()
   File(linkHfilePath).deleteRecursively()
   File(entHfilePath).deleteRecursively()
   File(louHfilePath).deleteRecursively()
 }*/

 "assembler" should {
   "create hfiles populated with expected enterprise data" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
     conf.set("hbase.zookeeper.quorum", "localhost")
     conf.set("hbase.zookeeper.property.clientPort", "2181")


     val actualHFileRows: List[HFileRow] = readEntitiesFromHFile[HFileRow](entHfilePath).collect.toList//.sortBy(_.ern)
     val actual = actualHFileRows.map(Enterprise(_)).sortBy(_.ern)
     val expected: List[Enterprise] = replaceDynamiclyGeneratedErns(actual).sortBy(_.ern).toList
     actual shouldBe expected


     spark.stop()

   }
 }



"assembler" should {
   "create hfiles populated with expected links data" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
     val confs = appConfs
     val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(_.cells.map(_.column).mkString)


     //HFileRow-s need to be flatMapped to HFileCell-s to avoid ordering mismatch on HFileRow.cells:
     val actualUpdated = assignStaticLinkIds(actual).flatMap(row => row.toHFileCells(confs.HBASE_ENTERPRISE_COLUMN_FAMILY))
     val expected = existingLinksForCreateNewPopulationScenarion.flatMap(row => row.toHFileCells(confs.HBASE_ENTERPRISE_COLUMN_FAMILY)).toSet
     actualUpdated shouldBe expected


     spark.close()
   }
 }


  "assembler" should {
   "create hfiles populated with expected local unit data" in {

         implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

         val actual: Seq[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList.sortBy(_.name)
         val actualUpdated = assignStaticIds(actual)
         val expected = lousForCreateNewPopulationScenario
         actualUpdated shouldBe expected


         spark.close()
       }
     }
}
