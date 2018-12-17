package closures

import java.io.{BufferedWriter, FileWriter}

import closures.mocks.{MockClosures, MockCreateNewPeriodHBaseDao}
import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import global.AppParams
import global.Configs.conf
import model.domain._
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import org.scalatest._
import spark.extensions.rdd.HBaseDataReader._
import utils.Paths
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario

import scala.reflect.io.File



class AddNewPeriodWithCalculationsSpec extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with HFileTestUtils{

  lazy val testDir = "newperiod"

  val cores:Int = Runtime.getRuntime().availableProcessors()

  object MockAssembleUnitsClosure extends AssembleUnitsClosure with MockClosures{

    override val hbaseDao = MockCreateNewPeriodHBaseDao

    override val ernMapping: Map[String, String] = Map(
      "NEW ENTERPRISE LU" -> newEntErn
    )

    override val lurnMapping: Map[String, String] = Map(
      "NEW ENTERPRISE LU" ->  newLouLurn
    )

    override val rurnMapping: Map[String, String] = Map(
      "NEW ENTERPRISE LU" ->  newRuRurn
    )
    override val prnMapping: Map[String, String] = Map(
      "NEW ENTERPRISE LU" ->  newRuPrn
    )

  }

/*  "dummy tests" should{

    "create test report files to make Jenkins happy" in{
      true shouldBe true
    }

  }*/

 val appConfs = AppParams(
   (Array[String](
     "LINKS", "ons", "l", linkHfilePath,
     "LEU", "ons", "d", leuHfilePath,
     "ENT", "ons", "d",entHfilePath,
     "LOU", "ons", "d",louHfilePath,
     "REU", "ons", "d",ruHfilePath,
     parquetPath,
     "201804",
     "HIVE DB NAME",
     "HIVE TABLE NAME",
     "HIVE SHORT TABLE NAME",
     payeFilePath,
     vatFilePath,
     "local",
     "add-calculated-period"
   )))


override def beforeAll() = {
  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
  conf.set("hbase.zookeeper.quorum", "localhost")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  withHbaseConnection { implicit connection:Connection =>
    createRecords(appConfs)
    ParquetDao.jsonToParquet(jsonFilePath)(spark, appConfs)
    //val existingDF = readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).collect
    MockAssembleUnitsClosure.createUnitsHfiles(appConfs)(spark, connection)
   }
  spark.stop
}


 override def afterAll() = {
   File(parquetPath).deleteRecursively()
   File(linkHfilePath).deleteRecursively()
   File(leuHfilePath).deleteRecursively()
   File(entHfilePath).deleteRecursively()
   File(louHfilePath).deleteRecursively()
   File(ruHfilePath).deleteRecursively()
   File(existingRecordsDir).deleteRecursively()
 }
/*  "create test-data csv" should {" just do it" in{
      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val geoPath = "/Users/vladshiligin/dev/ons/sbr-enterprise-assembler/src/test/resources/data/geo/test-dataset.csv"
      val pcPath = "src/test/resources/data/geo/postcodes.csv"
      val geoDF = spark.read.option("header", "true").csv(geoPath).select("pcds","rgn").toDF("postcode", "region")
      val pcDF = spark.read.option("header", "false").csv(pcPath).toDF("postcode")
      val rows = pcDF.join(geoDF, Seq("postcode"),"left_outer").collect()
      val recs = rows.map(row => { row.getAs[String]("postcode") + ","+row.getAs[String]("region") })
      val wholeSet = "postcode,region"+:recs
      val dataStr = wholeSet.mkString("\n")
      val file = new java.io.File("src/test/resources/data/geo/test-dataset.csv")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(dataStr)
      bw.close()
      true shouldBe true

}}*/

 "assembler" should {
 "create hfiles populated with expected enterprise data" in {

   implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
   val actualRows = readEntitiesFromHFile[HFileRow](entHfilePath).collect.toList
   val actual = actualRows.map(Enterprise(_)).sortBy(_.ern)
   val expected  = newPeriodEnts.sortBy(_.ern)
   actual shouldBe expected
   spark.stop()
 }
}

"assembler" should {
 "create hfiles populated with expected local units data" in {

   implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
   //val existing = readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).collect.toList.sortBy(_.key)
   val actual: List[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList.sortBy(_.lurn)
   val expected: List[LocalUnit] = newPeriodLocalUnits.sortBy(_.lurn)
   actual shouldBe expected
   spark.stop()
 }
}


"assembler" should {
   "create hfiles populated with expected reporting units data" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
     val existing = readEntitiesFromHFile[ReportingUnit](existingRusRecordHFiles).collect.toList
     //val actualHFileRows: List[HFileRow] = readEntitiesFromHFile[HFileRow](ruHfilePath).collect.toList
     val actual: List[ReportingUnit] = readEntitiesFromHFile[ReportingUnit](ruHfilePath).collect.toList.sortBy(_.rurn)
     val expected: List[ReportingUnit] = newPeriodReportingUnits.sortBy(_.rurn)
     actual shouldBe expected
     spark.stop()
   }
}

"assembler" should {
   "create hfiles populated with expected legal units data" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
    // val existingRecs = readEntitiesFromHFile[HFileRow](existingLeusRecordHFiles).collect.toList
     val existing = readEntitiesFromHFile[LegalUnit](existingLeusRecordHFiles).collect.toList.sortBy(_.ubrn)
     val actual: List[LegalUnit] = readEntitiesFromHFile[LegalUnit](leuHfilePath).collect.toList.sortBy(_.ubrn)
     val expected: List[LegalUnit] = newPeriodLegalUnits.sortBy(_.ubrn)
     actual shouldBe expected
     spark.stop()

   }
 }


 "assembler" should {
   "create hfiles populated with expected links data" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
     val confs = appConfs

     //val existing = readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).collect.toList.sortBy(_.key)
     val existingLous = readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).collect.toList.sortBy(_.key)
     val actualHFileRows: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(sortByKeyAndEntityName)
     val actualLinksRecords: Seq[LinkRecord] = LinkRecord.getLinks(actualHFileRows).sortBy(_.ern)
     val expectedLinks = expectedNewPeriodLinks.sortBy(_.ern)
     actualLinksRecords shouldBe expectedLinks
     spark.close()
   }
 }

def createRecords(appconf:AppParams)(implicit spark: SparkSession,connection:Connection) = {
  saveLinksToHFile(existingLinksForAddNewPeriodScenarion,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
  saveToHFile(existingLousForNewPeriodScenario,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
  saveToHFile(existingRusForNewPeriodScenario,appconf.HBASE_REPORTINGUNITS_COLUMN_FAMILY, appconf, existingRusRecordHFiles)
  saveToHFile(existingLeusForNewPeriodScenario,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingLeusRecordHFiles)
  saveToHFile(existingEntsForNewPeriodScenario,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingEntRecordHFiles)
}


}



