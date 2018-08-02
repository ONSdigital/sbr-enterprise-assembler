package closures

import closures.mocks.{MockClosures, MockCreateNewPeriodHBaseDao}
import dao.parquet.ParquetDao
import global.{AppParams, Configs}
import global.Configs.conf
import model.domain.{Enterprise, HFileRow, LinkRecord, LocalUnit}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import utils.{Paths, TestDataUtils}
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario

import scala.collection.immutable
import scala.reflect.io.File
import scala.util.Try

/**
  *
  */
class AddNewPeriodDataIntegrityTest extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with TestDataUtils{

  lazy val testDir = "newperiod"

  object MockCalculationsClosure extends NewPeriodWithCalculationsClosure{

    override val hbaseDao = MockCreateNewPeriodHBaseDao

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

  override def beforeAll() = {
    implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    createRecords(appConfs)(spark)
    ParquetDao.jsonToParquet(jsonFilePath)(spark, appConfs)
    MockCalculationsClosure.addNewPeriodDataWithCalculations(appConfs)(spark)
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
      val ents: Seq[Enterprise] = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList
      val links: Seq[LinkRecord] = readLinks
      val lous: Seq[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList
      spark.stop()
      checkIntegrity(ents,links,lous)

    }
  }

  def checkIntegrity(ents: Seq[Enterprise],links: Seq[LinkRecord],lous: Seq[LocalUnit]) = {
    val newErnFromEnt: String = ents.collect{case ent if(isNewId(ent.ern)) => ent.ern}.head
    val newLurnFromLinks: String = links.collect{case LinkRecord(`newErnFromEnt`,lurns,_) => lurns.find(isNewId)}.head.get
    val newLurnFromLou: String = lous.collect{case LocalUnit(lurn,_,`newErnFromEnt`,_,_,_,_,_,_,_,_,_,_,_) if(isNewId(lurn)) => lurn}.head
    newLurnFromLinks shouldBe newLurnFromLou
  }

  def isNewId(id:String) = id.startsWith("N")

  def readLinks(implicit spark:SparkSession) = {
    val actualHFileRows: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList
    LinkRecord.getLinkRecords(actualHFileRows)
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
