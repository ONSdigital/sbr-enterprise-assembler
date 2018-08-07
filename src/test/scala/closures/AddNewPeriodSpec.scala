package closures

import closures.mocks.{MockClosures, MockCreateNewPeriodHBaseDao}
import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDao
import global.Configs.conf
import global.{AppParams, Configs}
import model.domain.{Enterprise, HFileRow, LinkRecord, LocalUnit}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest._
import spark.extensions.rdd.HBaseDataReader._
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario
import utils.{Paths, TestDataUtils}

import scala.reflect.io.File
/**
  *
  */


class AddNewPeriodSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with TestDataUtils{

  lazy val testDir = "newperiod"

  object MockRefreshPeriodWithCalculationsClosure$ extends RefreshPeriodWithCalculationsClosure$ with MockClosures{

    override val hbaseDao = MockCreateNewPeriodHBaseDao

    override val ernMapping: Map[String, String] = Map(
      ("NEW ENTERPRISE LU" -> newEntErn)
    )

    override val lurnMapping: Map[String, String] = Map(
      ("NEW ENTERPRISE LU" ->  newLouLurn)
    )
  }

  val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "ENT", "ons", "d",entHfilePath,
      "LOU", "ons", "d",louHfilePath,
      parquetPath,
      "201804",payeFilePath,
      vatFilePath,
      "local",
      "add-calculated-period"
    )))



/*   override def beforeAll() = {
        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val confs = appConfs
        createRecords(confs)(spark)
        ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
        MockNewPeriodWithCalculationsClosure.addNewPeriodDataWithCalculations(appConfs)(spark)
        spark.stop()
  }*/
/*  override def afterAll() = {
        File(parquetPath).deleteRecursively()
        File(linkHfilePath).deleteRecursively()
        File(entHfilePath).deleteRecursively()
        File(louHfilePath).deleteRecursively()
        File(existingRecordsDir).deleteRecursively()
  }*/


  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val actualRows = readEntitiesFromHFile[HFileRow](entHfilePath).collect.toList
      val actual = actualRows.map(Enterprise(_)).sortBy(_.ern)
      val expected  = newPeriodEnts.sortBy(_.ern)
      actual shouldBe expected
      spark.stop()

    }
  }
/**
  * +------------------+-----------+---------+-------------+------------+------------+------------+------------+
  * |               ern|paye_empees|paye_jobs|cntd_turnover|app_turnover|std_turnover|grp_turnover|ent_turnover|
  * +------------------+-----------+---------+-------------+------------+------------+------------+------------+
  * |        2000000011|          2|        4|         null|        null|         390|        null|         390|
  * |        5000000011|          5|        5|         null|        null|        null|         444|           0|
  * |        4000000011|          4|        8|         null|         444|        null|         444|         444|
  * |        3000000011|         19|       20|          585|        null|        null|        null|         585|
  * |111111111-TEST-ERN|          3|        5|           85|        null|        null|        null|          85|
  * +------------------+-----------+---------+-------------+------------+------------+------------+------------+
  * */

"assembler" should {
    "create hfiles populated with expected local units data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      //val existing = readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).collect.toList.sortBy(_.key)
      val actual: List[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList.sortBy(_.lurn)
      val expected: List[LocalUnit] = newPeriodLocalUnits.sortBy(_.lurn)
      actual shouldBe expected
      spark.stop()

    }
  }


    "assembler" should {
    "create hfiles populated with expected links data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
      val confs = appConfs
      val existing = readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).collect.toList.sortBy(_.key)
      val existingLous = readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).collect.toList.sortBy(_.key)
      val actualHFileRows: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(sortByKeyAndEntityName)
      val actualLinksRecords: Seq[LinkRecord] = LinkRecord.getLinkRecords(actualHFileRows).sortBy(_.ern)
      val expectedHFileRows: Seq[HFileRow] = newPeriodLinks.sortBy(sortByKeyAndEntityName)
      val expectedlLinksRecords: Seq[LinkRecord] = LinkRecord.getLinkRecords(expectedHFileRows).sortBy(_.ern)
      actualLinksRecords shouldBe expectedlLinksRecords
      spark.close()
    }
  }

  def sortByKeyAndEntityName(row:HFileRow) = {

      val keyBlocks= row.key.split("~")
      val id = keyBlocks.head
      val entityName = keyBlocks.tail.head
      entityName+id
  }

  def saveToHFile(rows:Seq[HFileRow], colFamily:String, appconf:AppParams, path:String)(implicit spark:SparkSession) = {
    val records: RDD[HFileRow] = spark.sparkContext.parallelize(rows)
    val cells: RDD[(String, hfile.HFileCell)] = records.flatMap(_.toHFileCellRow(colFamily))
    cells.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(path,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)
  }

  def createRecords(appconf:AppParams)(implicit spark:SparkSession) = {
    saveToHFile(existingLousForNewPeriodScenario,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
    saveToHFile(existingLinksForAddNewPeriodScenarion,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
    saveToHFile(ents,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingEntRecordHFiles)
  }


}


