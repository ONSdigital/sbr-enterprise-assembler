package closures

import closures.mocks.MockCreateNewPeriodHBaseDao
import dao.hbase.HBaseConnectionManager
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
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario
import utils.Paths
import utils.data.consistency.DataConsistencyCheck

import scala.reflect.io.File

/**
  *
  */
class AddNewPeriodDataIntegrityTest extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with DataConsistencyCheck with HFileTestUtils{

  lazy val testDir = "newperiod"

  object MockClosure extends AssembleUnitsClosure{

    override val hbaseDao = MockCreateNewPeriodHBaseDao

/*    override def  getAllLOUs(allEntsDF:DataFrame,appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
      val numberOfPartitions = allEntsDF.rdd.getNumPartitions
      val res = super.getAllLOUs(allEntsDF,appconf,confs)
      res.coalesce(numberOfPartitions)
    }

    override def getAllLUsDF(appconf: AppParams)(implicit spark: SparkSession) = {

      val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)
      val numberOfPartitions = incomingBiDataDF.rdd.getNumPartitions
      val existingLEsDF: DataFrame = getExistingLeusDF(appconf, Configs.conf)


      val joinedLUs = incomingBiDataDF.join(
        existingLEsDF.withColumnRenamed("ubrn", "id").select("id", "ern"),
        Seq("id"), "left_outer")

      getAllLUs(joinedLUs, appconf).coalesce(numberOfPartitions)
    }

    override def getAllEntsCalculated(allLUsDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {
      val numberOfPartitions = allLUsDF.rdd.getNumPartitions
      super.getAllEntsCalculated(allLUsDF, appconf).coalesce(numberOfPartitions)
    }*/
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
      "HIVE DB NAME",
      "HIVE TABLE NAME",
      "HIVE_SHORT_TABLE_NAME",
      payeFilePath,
      vatFilePath,
      "local",
      "add-calculated-period"
    )))

/*  "dummy tests" should{

    "create report files to make Jenkins happy" in{
      true shouldBe true
    }

  }*/

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

  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val ents: Seq[Enterprise] = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList
      val links: Seq[LinkRecord] = readLinks
      val lous: Seq[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList
      spark.stop()
      val r = areAllIdsMatch(ents,links,lous)
      checkIntegrity(ents,links,lous)

    }
  }

  def areAllIdsMatch(ents: Seq[Enterprise], links: Seq[LinkRecord], lous: Seq[LocalUnit]) = {
    val entsErns: Seq[String] = ents.map(_.ern).sorted
    val louLurns: Seq[String] = lous.map(_.lurn).sorted
    val louErns: Seq[String] = lous.map(_.ern).distinct.sorted
    val linksErns: Seq[String] = links.map(_.ern).distinct.sorted
    val linksLurns: Seq[String] = links.map(_.rus.map(_.lurns)).flatten.flatten.distinct.sorted

    val r1 = entsErns==louErns
    val r2 = louErns==linksErns
    val r3 = entsErns==linksErns

    val areErnsGood = r1 && r2 && r3
    val areLurnsGood = louLurns==linksLurns

    areErnsGood && areLurnsGood

  }

  def checkIntegrity(ents: Seq[Enterprise],links: Seq[LinkRecord],lous: Seq[LocalUnit]) = {
    val newErnFromEnt: String = ents.collect{case ent if(isNewId(ent.ern)) => ent.ern}.head
    val newLurnFromLinks: String = links.collect{case LinkRecord(`newErnFromEnt`,rus,_) => rus.map(_.lurns).flatten.find(isNewId)}.head.get
    val newLurnFromLou: String = lous.collect{case LocalUnit(lurn,_,`newErnFromEnt`,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) if(isNewId(lurn)) => lurn}.head
    newLurnFromLinks shouldBe newLurnFromLou
  }

  def isNewId(id:String) = id.startsWith("N") || id.contains("-TEST-")

  def readLinks(implicit spark:SparkSession) = {
    val actualHFileRows: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList
    LinkRecord.getLinks(actualHFileRows)
  }

  def createRecords(appconf:AppParams)(implicit spark: SparkSession,connection:Connection) = {
    saveLinksToHFile(existingLinksForAddNewPeriodScenarion,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
    saveToHFile(existingLousForNewPeriodScenario,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
    saveToHFile(existingRusForNewPeriodScenario,appconf.HBASE_REPORTINGUNITS_COLUMN_FAMILY, appconf, existingRusRecordHFiles)
    saveToHFile(existingLeusForNewPeriodScenario,appconf.HBASE_LEGALUNITS_COLUMN_FAMILY, appconf, existingLeusRecordHFiles)
    saveToHFile(existingEntsForNewPeriodScenario,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingEntRecordHFiles)
  }

}
