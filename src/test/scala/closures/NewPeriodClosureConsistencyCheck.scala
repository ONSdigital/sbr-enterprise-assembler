package closures

import closures.mocks.{MockCreateNewPeriodHBaseDao, MockDataReader}
import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import global.{AppParameters, AppParams}
import global.Configs.conf
import model.domain._
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import utils.Paths
import utils.data.consistency.DataConsistencyCheck
import utils.data.existing.ExistingData

import scala.reflect.io.File

class NewPeriodClosureConsistencyCheck extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with DataConsistencyCheck with HFileTestUtils {

  lazy val testDir = "newperiod"

  val cores: Int = Runtime.getRuntime.availableProcessors()

  object MockClosure extends AssembleUnitsClosure with MockDataReader {
    override val hbaseDao: MockCreateNewPeriodHBaseDao.type = MockCreateNewPeriodHBaseDao
  }

  val appConfs: AppParameters = AppParameters(
    Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "LEU", "ons", "d", leuHfilePath,
      "ENT", "ons", "d", entHfilePath,
      "LOU", "ons", "d", louHfilePath,
      "REU", "ons", "d", ruHfilePath,
      parquetPath,
      "201804",
      "HIVE DB NAME",
      "HIVE TABLE NAME",
      "HIVE_SHORT_TABLE_NAME",
      payeFilePath,
      vatFilePath,
      "local",
      "add-calculated-period"
    ))

  override def beforeAll(): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().master(s"local[*]").appName("enterprise assembler").getOrCreate()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    AppParams.setConfiguration(appConfs)

    withHbaseConnection { implicit connection: Connection =>
      createRecords
      ParquetDao.jsonToParquet(jsonFilePath)(spark)
      MockClosure.createUnitsHfiles(spark, connection)
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
    saveLinksToHFile(existingLinksForAddNewPeriodScenarion, AppParams.HBASE_LINKS_COLUMN_FAMILY, existingLinksRecordHFiles)
    saveToHFile(existingLousForNewPeriodScenario, AppParams.HBASE_LOCALUNITS_COLUMN_FAMILY, existingLousRecordHFiles)
    saveToHFile(existingRusForNewPeriodScenario, AppParams.HBASE_REPORTINGUNITS_COLUMN_FAMILY, existingRusRecordHFiles)
    saveToHFile(existingLeusForNewPeriodScenario, AppParams.HBASE_LEGALUNITS_COLUMN_FAMILY, existingLeusRecordHFiles)
    saveToHFile(existingEntsForNewPeriodScenario, AppParams.HBASE_ENTERPRISE_COLUMN_FAMILY, existingEntRecordHFiles)
  }
}
