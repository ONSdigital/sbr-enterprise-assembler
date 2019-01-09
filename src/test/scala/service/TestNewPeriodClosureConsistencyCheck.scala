package service

import dao.hbase.HBaseConnectionManager
import dao.hbase.HBaseDataReader.readEntitiesFromHFile
import dao.parquet.ParquetDao
import model._
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import util.configuration.{Config, AssemblerConfiguration, AssemblerOptions}
import utils.Paths
import utils.data.HFileTestUtils
import utils.data.consistency.DataConsistencyCheck
import utils.data.existing.ExistingData

import scala.reflect.io.File

class TestNewPeriodClosureConsistencyCheck extends Paths with WordSpecLike with Matchers
  with BeforeAndAfterAll with ExistingData with DataConsistencyCheck with HFileTestUtils {

  lazy val testDir = "newperiod"

  val cores: Int = Runtime.getRuntime.availableProcessors()

  object MockUnits extends AssembleUnits

  override def beforeAll(): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master(s"local[*]")
      .appName("enterprise assembler")
      .getOrCreate()

    afterAll()

    HBaseConnectionManager.withHbaseConnection {
      implicit connection: Connection =>
        createRecords
        ParquetDao.jsonToParquet(jsonFilePath)(spark)
        MockUnits.createUnitsHfiles(spark, connection)
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

      implicit val spark: SparkSession = SparkSession.builder().master(s"local[*]").appName("enterprise assembler").getOrCreate()
      val ents = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList
      val lous = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList
      val leus = readEntitiesFromHFile[LegalUnit](leuHfilePath).collect.toList
      val rus = readEntitiesFromHFile[ReportingUnit](ruHfilePath).collect.toList
      val links: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList
      val res = checkIntegrity(ents, lous, leus, rus, links)
      res shouldBe true
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
