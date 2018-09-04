package utils.data.consistency

import dao.hbase.HBaseConnectionManager
import model.domain._
/**
  *
  */


trait DataConsistencyCheck extends HBaseConnectionManager{



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
      (ids.head=="LOU" && ids.last==lou.lurn) && (row.cells.exists(cell => cell.column==s"p_ENT" && cell.value==lou.ern))
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
      (ids.head=="LEU" && ids.last==leu.ubrn) && (row.cells.exists(cell => cell.column=="p_ENT" && cell.value==leu.ern))
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

    def hasParentEntRef(ru:ReportingUnit) = links.exists(row => {
      val ids = row.key.split("~")
      (ids.head=="REU" && ids.last==ru.rurn) && (row.cells.exists(cell => cell.column==s"p_ENT" && cell.value==ru.ern))
    })

    val inConsistentLeus = rus.filterNot(ru => {
      registeredWithParentEnt(ru) && hasParentEntRef(ru)
    })

    inConsistentLeus.isEmpty
  }


  def checkLousAgainstRus(rus:Seq[ReportingUnit], links:Seq[HFileRow]) = {


    def childLouLurns(ru:ReportingUnit) = {
      val key = s"REU~${ru.rurn}"
      links.collect { case HFileRow(`key`, cells) =>
        cells.collect { case KVCell(column, "LOU") => column.stripPrefix("c_") }
      }.flatten
    }

    def parentLurns(ru:ReportingUnit) = links.collect{case row if(row.key.startsWith("LOU")) =>
                            row.cells.collect{case KVCell("p_REU", ru.rurn) => row.key.stripPrefix("LOU~")}}.flatten


    val inConsistentLeus = rus.filterNot(ru => {
      val children = childLouLurns(ru).toSet
      val parents = parentLurns(ru).toSet
      children.equals(parents)
    })
    inConsistentLeus.isEmpty
  }




}
