package model

import dao.hbase.HBaseDataReader
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, KeyValue}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import util.configuration.AssemblerConfiguration

import scala.language.implicitConversions
import scala.util.Try

case class HFileRow(key: String, cells: Iterable[KVCell[String, String]]) {

  def getLinkId: String = key.split("~").last

  def getValueOrNull(key: String, byKey: Boolean = true): String = getCellValue(key, byKey).orNull

  def getValueOrStr(key: String, byKey: Boolean = true, default: String = ""): String = getCellValue(key, byKey).getOrElse(default)

  def getCellValue(key: String, byKey: Boolean): Option[String] = if (byKey) cells.collectFirst { case KVCell(`key`, value) => Option(value) }.flatten //.getOrElse(null)
  else cells.collectFirst { case KVCell(value, `key`) => Option(value) }.flatten //.getOrElse(null)

  def getCellArrayValue(key: String): Iterable[String] = {
    val result = cells.collect { case KVCell(value, `key`) => value }
    if (result.isEmpty) null else result
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case HFileRow(otherKey, otherCells) if (otherKey == this.key) && (this.cells.toSet == otherCells.toSet) => true
    case _ => false
  }

  //put type set as default
  def toHFileCells(columnFamily: String, kvType: Int = KeyValue.Type.Put.ordinal()): Iterable[HFileCell] = cells.map(cell => HFileCell(key, columnFamily, cell.column, cell.value, kvType))

  def toEntRow: GenericRowWithSchema = {

    try {
      new GenericRowWithSchema(Array(getValueOrStr("ern"),
        getValueOrStr("prn", default = AssemblerConfiguration.DefaultPRN),
        getValueOrNull("entref"),
        getValueOrStr("name"),
        getValueOrNull("trading_style"),
        getValueOrStr("address1"),
        getValueOrNull("address2"),
        getValueOrNull("address3"),
        getValueOrNull("address4"),
        getValueOrNull("address5"),
        getValueOrStr("postcode"),
        getValueOrStr("sic07"),
        getValueOrStr("legal_status"),
        getValueOrStr("working_props", default = AssemblerConfiguration.DefaultWorkingProps)
      ), Schemas.entRowSchema)
    } catch {
      case e: java.lang.RuntimeException =>
        println(s"(toEntRow)Exception reading enterprise row with ern: ${getValueOrStr("ern")}")
        throw e
    }
  }

  def toLeuLinksRow: GenericRowWithSchema = {

    new GenericRowWithSchema(Array(
      getLinkId,
      getValueOrStr("p_ENT"),
      getCellValue("CH", byKey = false).map(_.substring(2)).orNull,
      Try {
        getCellArrayValue("PAYE").map(paye => if (paye.startsWith("c_")) {
          paye.substring(2)
        } else paye)
      }.getOrElse(null),
      Try {
        getCellArrayValue("VAT").map(vat => if (vat.startsWith("c_")) {
          vat.substring(2)
        } else vat)
      }.getOrElse(null)
    ), Schemas.linksLeuRowSchema)
  }

  def toHFileCellRow(colFamily: String): Iterable[(String, HFileCell)] = {
    cells.map(cell => (key, HFileCell(key, colFamily, cell.column, cell.value)))
  }
}

object HFileRow {

  def apply(entry: (String, Iterable[(String, String)])): HFileRow = new HFileRow(entry._1, entry._2.map(c => KVCell(c)).toSeq)

  def apply(result: Result): HFileRow = {
    val rowKey = Bytes.toString(result.getRow)
    val cells: Array[(String, String)] = result.rawCells().map(c => HBaseDataReader.getKeyValue(c)._2)
    new HFileRow(rowKey, cells.map(cell => KVCell(cell._1.trim(), cell._2.trim())))
  }

  implicit def buildFromHFileDataMap(entry: (String, Iterable[(String, String)])): HFileRow = HFileRow(entry)
}