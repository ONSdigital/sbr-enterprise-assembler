package closures

import closures.mocks.MockNewPeriodClosure
import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import global.{AppParams, Configs}
import model.domain.{Enterprise, HFileRow, LocalUnit}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario
import utils.Paths

import scala.reflect.io.File
/**
  *
  *
  */
class NewPeriodClosureSpec extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario {

  lazy val testDir = "calculations"

  object MockNewPeriodClosure extends MockNewPeriodClosure{
        lazy val testDir = "calculations"
  }

  val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "LEU", "ons", "d", leuHfilePath,
      "ENT", "ons", "d",entHfilePath,
      "LOU", "ons", "d",louHfilePath,
      "REU", "ons", "d",ruHfilePath,
      parquetPath,
      "201804",
      payeFilePath,
      vatFilePath,
      geoFilePath,
      "local",
      "addperiod"
    )))

  /* override def beforeAll() = {
      val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val confs = appConfs
      createRecords(confs)(spark)
      ParquetDao.jsonToParquet(jsonOrigFilePath)(spark, confs)
      withHbaseConnection { implicit connection: Connection =>
        MockNewPeriodClosure.createUnitsHfiles(appConfs)(spark,connection)
      }
      spark.stop()
    }

    override def afterAll() = {
        File(parquetPath).deleteRecursively()
        File(linkHfilePath).deleteRecursively()
        File(entHfilePath).deleteRecursively()
        File(louHfilePath).deleteRecursively()
        File(existingRecordsDir).deleteRecursively()
    }

    "assembler" should {
      "create hfiles populated with expected enterprise data" in {

        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val existingEnts = readEntitiesFromHFile[HFileRow](existingEntRecordHFiles).collect.toList.sortBy(_.key)
        val actualRows = readEntitiesFromHFile[HFileRow](entHfilePath).collect.toList
        val actual: List[Enterprise] = actualRows.map(Enterprise(_)).sortBy(_.ern)
        val expected: List[Enterprise] = newPeriodEntsWithoutCalculations.sortBy(_.ern)
        actual shouldBe expected
        spark.stop()
      }
    }

    "assembler" should {
      "create hfiles populated with expected local units data" in {
        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val actual: List[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList.sortBy(_.lurn)
        val expected: List[LocalUnit] = newPeriodLocalUnitsWithoutCalculations.sortBy(_.lurn)
        actual shouldBe expected
        spark.stop()
      }
    }*/


  def saveToHFile(rows:Seq[HFileRow], colFamily:String, appconf:AppParams, path:String)(implicit spark:SparkSession) = {
    val records: RDD[HFileRow] = spark.sparkContext.parallelize(rows)
    val cells: RDD[(String, hfile.HFileCell)] = records.flatMap(_.toHFileCellRow(colFamily))
    cells.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)
  }

  def createRecords(appconf:AppParams)(implicit spark:SparkSession) = {
    saveToHFile(existingEntsForNewPeriodScenario,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingEntRecordHFiles)
    saveToHFile(existingLinksForAddNewPeriodScenarion,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
    saveToHFile(existingLousForNewPeriodScenario,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
  }
}
