package utils.data.consistency

import closures.BaseClosure
import closures.mocks.{MockCreateNewPeriodHBaseDao, MockDataReader}
import dao.hbase.{HBaseConnectionManager, HFileUtils}
import model.domain._
import org.scalatest._
import utils.Paths
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario
/**
  *
  */


class DataConsistencyCheck[T<:BaseClosure](val testDir:String, closure:T) extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario{

  object MockClosure extends T with HFileUtils with MockDataReader{this:T =>
    override val hbaseDao = MockCreateNewPeriodHBaseDao
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