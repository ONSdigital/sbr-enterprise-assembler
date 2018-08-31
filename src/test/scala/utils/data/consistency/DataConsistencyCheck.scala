package utils.data.consistency

import closures.BaseClosure
import closures.mocks.{MockCreateNewPeriodHBaseDao, MockDataReader}
import dao.hbase.{HBaseConnectionManager, HFileUtils}
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
/**
  *
  */


class DataConsistencyCheck[T<:BaseClosure](val testDir:String, closure:T) extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario{

  object MockClosure extends T with HFileUtils with MockDataReader{this:T =>
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
    //val existingDF = readEntitiesFromHFile[HFileRow](existingRusRecordHFiles).collect
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    withHbaseConnection { implicit connection:Connection =>
      MockClosure.createUnitsHfiles(appConfs)(spark, connection)
    }
    spark.stop

  }
  override def afterAll() = {
    File(linkHfilePath).deleteRecursively()
    File(leuHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
    File(louHfilePath).deleteRecursively()
  }

  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val  ents = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList
      val  lous = readEntitiesFromHFile[LocalUnit](entHfilePath).collect.toList
      val  leus = readEntitiesFromHFile[LegalUnit](leuHfilePath).collect.toList
      val  rus = readEntitiesFromHFile[ReportingUnit](ruHfilePath).collect.toList
      val links: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList
      val res = checkIntegrity(ents, lous, leus, rus,links )
      res shouldBe true
      spark.stop()
    }
  }

  def checkIntegrity(ents:Seq[Enterprise], lous:Seq[LocalUnit],leus:Seq[LegalUnit],rus:Seq[ReportingUnit], links:Seq[HFileRow]) = {
    val lousOK = checkLous(lous, links)
    val leusOK = checkLeus(leus, links)
    val rusOK = checkRus(rus, links)
    val rusVsLousOK = checkLousAgainstRus(rus, links)
    lousOK && leusOK && rusOK && rusVsLousOK
  }

  def checkLous(lous:Seq[LocalUnit], links:Seq[HFileRow]) = {


    def registeredWithParentEnt(lou:LocalUnit) = links.exists(row => {
      val ids = row.key.split("~")
      (ids.head=="ENT" && ids.last==lou.ern) && (row.cells.exists(_.column==s"c_${lou.lurn}"))
    })

    def hasParentEntRef(lou:LocalUnit) = links.exists(row => {
      val ids = row.key.split("~")
      (ids.head=="LOU" && ids.last==lou.lurn) && (row.cells.exists(_.column==s"p_${lou.ern}"))
    })

    val notConsistentLous = lous.filterNot(lou => {
      registeredWithParentEnt(lou) && hasParentEntRef(lou)
    })

    notConsistentLous.isEmpty
  }

  def checkLeus(leus:Seq[LegalUnit], links:Seq[HFileRow]) = {


    def registeredWithParentEnt(leu:LegalUnit) = links.exists(row => {
      val ids = row.key.split("~")
      (ids.head=="ENT" && ids.last==leu.ern) && (row.cells.exists(_.column==s"c_${leu.ubrn}"))
    })

    def hasParentEntRef(leu:LegalUnit) = links.exists(row => {
      val ids = row.key.split("~")
      (ids.head=="LEU" && ids.last==leu.ubrn) && (row.cells.exists(_.column==s"p_${leu.ern}"))
    })

    val notConsistentLeus = leus.filterNot(leu => {
      registeredWithParentEnt(leu) && hasParentEntRef(leu)
    })

    notConsistentLeus.isEmpty
  }


  def checkRus(rus:Seq[ReportingUnit], links:Seq[HFileRow]) = {


    def registeredWithParentEnt(ru:ReportingUnit) = links.exists(row => {
      val ids = row.key.split("~")
      (ids.head=="ENT" && ids.last==ru.ern) && (row.cells.exists(_.column==s"c_${ru.rurn}"))
    })

    def hasParentEntRef(leu:ReportingUnit) = links.exists(row => {
      val ids = row.key.split("~")
      (ids.head=="LEU" && ids.last==leu.rurn) && (row.cells.exists(_.column==s"p_${leu.ern}"))
    })

    val inConsistentLeus = rus.filterNot(ru => {
      registeredWithParentEnt(ru) && hasParentEntRef(ru)
    })

    inConsistentLeus.isEmpty
  }


  def checkLousAgainstRus(rus:Seq[ReportingUnit], links:Seq[HFileRow]) = {


    def childLouLurns(ru:ReportingUnit) = links.collect{case HFileRow("REU~"+ru.rurn,cells) =>
                                          cells.collect{case KVCell(column, "LOU") => column.stripPrefix("c_")}}.flatten

    def parentLurns(ru:ReportingUnit) = links.collect{case row if(row.key.startsWith("LOU")) =>
                            row.cells.collect{case KVCell("p_REU", ru.rurn) => row.key.stripPrefix("LOU~")}}.flatten


    val inConsistentLeus = rus.filterNot(ru => {
      val children = childLouLurns(ru)
      val parents = parentLurns(ru)
      children.equals(parents)
    })
    inConsistentLeus.isEmpty
  }




}
