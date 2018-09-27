package closures

import closures.mocks.{MockCreateNewPeriodHBaseDao, MockDataReader}
import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import global.AppParams
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

class NewPeriodClosureConsistencyCheck  extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with DataConsistencyCheck with HFileTestUtils{

  lazy val testDir = "newperiod"

  object MockClosure extends RefreshPeriodWithCalculationsClosure with MockDataReader{
    override val hbaseDao = MockCreateNewPeriodHBaseDao
  }

  val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "LEU", "ons", "d", leuHfilePath,
      "ENT", "ons", "d",entHfilePath,
      "LOU", "ons", "d",louHfilePath,
      "REU", "ons", "d",ruHfilePath,
      parquetPath,
      "201804",payeFilePath,
      vatFilePath,
      "local",
      "add-calculated-period"
    )))


  override def beforeAll() = {
    implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    withHbaseConnection { implicit connection:Connection =>
        createRecords(appConfs)
        ParquetDao.jsonToParquet(jsonFilePath)(spark, appConfs)
          //val existingDF = readEntitiesFromHFile[HFileRow](existingRusRecordHFiles).collect

        MockClosure.createUnitsHfiles(appConfs)(spark, connection)
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


/*  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val  ents = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList
      val  lous = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList
      val  leus = readEntitiesFromHFile[LegalUnit](leuHfilePath).collect.toList
      val  rus = readEntitiesFromHFile[ReportingUnit](ruHfilePath).collect.toList
      val links: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList
      val res = checkIntegrity(ents, lous, leus, rus,links )
      res shouldBe true
      spark.stop()
    }
  }*/


  def createRecords(appconf:AppParams)(implicit spark: SparkSession,connection:Connection) = {
    saveLinksToHFile(existingLinksForAddNewPeriodScenarion,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
    saveToHFile(existingLousForNewPeriodScenario,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
    saveToHFile(existingRusForNewPeriodScenario,appconf.HBASE_REPORTINGUNITS_COLUMN_FAMILY, appconf, existingRusRecordHFiles)
    saveToHFile(existingLeusForNewPeriodScenario,appconf.HBASE_LEGALUNITS_COLUMN_FAMILY, appconf, existingLeusRecordHFiles)
    saveToHFile(existingEntsForNewPeriodScenario,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingEntRecordHFiles)
  }
}
