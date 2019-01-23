package service

import service.mocks.MockAssembleUnits
import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import org.scalatest._
import dao.hbase.HBaseDataReader._
import model._
import util.configuration.{AssemblerConfiguration}
import utils.Paths
import utils.data.HFileTestUtils
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedCalculations

import scala.reflect.io.File

class TestCalculations extends Paths with WordSpecLike with Matchers
  with BeforeAndAfterAll with ExistingData  with ExpectedCalculations with HFileTestUtils {

  lazy val testDir = "newperiod"

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
        MockAssembleUnits.createHfiles(spark, connection)
    }
    spark.stop
  }

  override def afterAll(): Unit = {
    File(parquetPath).deleteRecursively()
    File(adminDataHfilePath).deleteRecursively()
    File(regionHfilePath).deleteRecursively()
    File(employmentHfilePath).deleteRecursively()
    File(linkHfilePath).deleteRecursively()
    File(existingRecordsDir).deleteRecursively()
  }

  "method service" should {
    "Calculate Admin Data" in {
      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Registers Method Service").getOrCreate()
      val actual = readEntitiesFromHFile[AdminData](adminDataHfilePath).collect.toList.sortBy(_.ern)
      val expected = expectedAdminDataCalculations.sortBy(_.ern)

      actual shouldBe expected
      spark.stop()
    }
  }

  "method service" should {
    "Calculate Region Data" in {
      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Registers Method Service").getOrCreate()
      val actual = readEntitiesFromHFile[Region](regionHfilePath).collect.toList.sortBy(_.ern)
      val expected = expectedRegionCalculations.sortBy(_.ern)

      actual shouldBe expected
      spark.stop()
    }
  }

  "method service" should {
    "Calculate Employment Data" in {
      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Registers Method Service").getOrCreate()
      val actual = readEntitiesFromHFile[Employment](employmentHfilePath).collect.toList.sortBy(_.ern)
      val expected = expectedEmploymentCalculations.sortBy(_.ern)

      actual shouldBe expected
      spark.stop()
    }
  }

  def createRecords()(implicit spark: SparkSession, connection: Connection): Unit = {
    saveLinksToHFile(existingLinksForAddNewPeriodScenarion, AssemblerConfiguration.HBaseLinksColumnFamily, existingLinksRecordHFiles)
    saveToHFile(existingEntsForNewPeriodScenario, AssemblerConfiguration.HBaseEnterpriseColumnFamily, existingEntRecordHFiles)
  }

}



