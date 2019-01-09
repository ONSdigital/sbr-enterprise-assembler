package service

import service.mocks.MockAssembleUnits
import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import org.scalatest._
import dao.hbase.HBaseDataReader._
import model._
import util.configuration.{Config, AssemblerConfiguration, AssemblerOptions}
import utils.Paths
import utils.data.HFileTestUtils
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario

import scala.reflect.io.File

class TestAddNewPeriodWithCalculations extends  Paths with WordSpecLike with Matchers
  with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with HFileTestUtils {

  lazy val testDir = "newperiod"

  val cores: Int = Runtime.getRuntime.availableProcessors()

  override def beforeAll(): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("enterprise assembler")
      .getOrCreate()

    afterAll()

    HBaseConnectionManager.withHbaseConnection {
      implicit connection: Connection =>
        createRecords
        ParquetDao.jsonToParquet(jsonFilePath)(spark)
        MockAssembleUnits.createUnitsHfiles(spark, connection)
    }
    spark.stop
  }

  override def afterAll(): Unit = {
    File(parquetPath).deleteRecursively()
    File(linkHfilePath).deleteRecursively()
    File(leuHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
    File(louHfilePath).deleteRecursively()
    File(ruHfilePath).deleteRecursively()
    File(existingRecordsDir).deleteRecursively()
  }

  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
      val actualRows = readEntitiesFromHFile[HFileRow](entHfilePath).collect.toList
      val actual = actualRows.map(Enterprise(_)).sortBy(_.ern)
      val expected = newPeriodEnts.sortBy(_.ern)
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
      //val existing = readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).collect.toList.sortBy(_.key)
      val existingLous = readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).collect.toList.sortBy(_.key)
      val actualHFileRows: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(sortByKeyAndEntityName)
      val actualLinksRecords: Seq[LinkRecord] = LinkRecord.getLinks(actualHFileRows).sortBy(_.ern)
      val expectedLinks = expectedNewPeriodLinks.sortBy(_.ern)
      actualLinksRecords shouldBe expectedLinks
      spark.stop()
    }
  }

  def createRecords()(implicit spark: SparkSession, connection: Connection): Unit = {
    saveLinksToHFile(existingLinksForAddNewPeriodScenarion, AssemblerConfiguration.HBaseLinksColumnFamily, existingLinksRecordHFiles)
    saveToHFile(existingLousForNewPeriodScenario, AssemblerConfiguration.HBaseLocalUnitsColumnFamily, existingLousRecordHFiles)
    saveToHFile(existingRusForNewPeriodScenario, AssemblerConfiguration.HBaseReportingUnitsColumnFamily, existingRusRecordHFiles)
    saveToHFile(existingLeusForNewPeriodScenario, AssemblerConfiguration.HBaseLegalUnitsColumnFamily, existingLeusRecordHFiles)
    saveToHFile(existingEntsForNewPeriodScenario, AssemblerConfiguration.HBaseEnterpriseColumnFamily, existingEntRecordHFiles)
  }

}



