package model.domain

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, KeyValue}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.extensions.rdd.HBaseDataReader
import util.options.ConfigOptions

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

    import spark.extensions.sql._

    try {
      new GenericRowWithSchema(Array(getValueOrStr("ern"),
        getValueOrStr("prn", default = ConfigOptions.DefaultPRN),
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
        getValueOrStr("working_props", default = ConfigOptions.DefaultWorkingProps)
      ), entRowSchema)
    } catch {
      case e: java.lang.RuntimeException => {
        println(s"(toEntRow)Exception reading enterprise row with ern: ${getValueOrStr("ern")}")
        throw e
      }
    }
  }

  def toLeuRow: GenericRowWithSchema = {

    import spark.extensions.sql._

    try {
      new GenericRowWithSchema(Array(

        getValueOrStr("ubrn"),
        key.split("~").head.reverse,
        //getValueOrStr("ern"),
        getValueOrStr("prn"),
        getValueOrNull("crn"),
        getValueOrStr("name"),
        getValueOrNull("trading_style"),
        getValueOrStr("address1"),
        getValueOrNull("address2"),
        getValueOrNull("address3"),
        getValueOrNull("address4"),
        getValueOrNull("address5"),
        getValueOrStr("postcode"),
        getValueOrStr("sic07"),
        getValueOrNull("paye_jobs"),
        getValueOrNull("turnover"),
        getValueOrStr("legal_status"),
        getValueOrNull("trading_status"),
        getValueOrStr("birth_date"),
        getValueOrNull("death_date"),
        getValueOrNull("death_code"),
        getValueOrNull("uprn")
      ), leuRowSchema)
    } catch {
      case e: java.lang.RuntimeException => {
        println(s"(toLuRow)Exception reading enterprise row with ern: ${getValueOrStr("ern")}")
        throw e
      }
    }
  }

  def toRuRow: GenericRowWithSchema = {

    import spark.extensions.sql._

    new GenericRowWithSchema(Array(
      getValueOrStr("rurn"),
      getValueOrStr("ern"),
      getValueOrStr("name"),
      getValueOrNull("entref"),
      getValueOrNull("ruref"),
      getValueOrNull("trading_style"),
      getValueOrStr("legal_status"),
      getValueOrStr("address1"),
      getValueOrNull("address2"),
      getValueOrNull("address3"),
      getValueOrNull("address4"),
      getValueOrNull("address5"),
      getValueOrStr("postcode"),
      getValueOrStr("sic07"),
      getValueOrStr("employees"),
      getValueOrStr("turnover"),
      getValueOrStr("prn"),
      getValueOrStr("region"),
      getValueOrStr("employment", default = "0")
    ), ruRowSchema)
  }

  def toLeuLinksRow: GenericRowWithSchema = {

    import spark.extensions.sql._

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
    ), linksLeuRowSchema)
  }

  def toUbrnErnRow: GenericRowWithSchema = {

    import spark.extensions.sql._

    new GenericRowWithSchema(Array(
      getLinkId,
      getValueOrStr("p_ENT")
    ), existingLuBiRowSchema)
  }

  def toLouRow: GenericRowWithSchema = {
    import spark.extensions.sql._

    try {
      new GenericRowWithSchema(Array(

        getValueOrStr("lurn"),
        getValueOrNull("luref"),
        getValueOrStr("ern"),
        getValueOrStr("prn"),
        getValueOrStr("rurn"),
        getValueOrNull("ruref"),
        getValueOrStr("name"),
        getValueOrNull("entref"),
        getValueOrNull("trading_style"),
        getValueOrStr("address1"),
        getValueOrNull("address2"),
        getValueOrNull("address3"),
        getValueOrNull("address4"),
        getValueOrNull("address5"),
        getValueOrStr("postcode"),
        getValueOrStr("region"),
        getValueOrStr("sic07"),
        getValueOrNull("employees"),
        getValueOrStr("employment", default = "0")
      ), louRowSchema)
    } catch {
      case e: java.lang.RuntimeException => {
        println(s"Exception reading enterprise row with ern: ${getValueOrStr("ern")}")
        throw e
      }
    }
  }

  def toHFileCellRow(colFamily: String): Iterable[(String, HFileCell)] = {
    cells.map(cell => (key, HFileCell(key, colFamily, cell.column, cell.value)))
  }

  def toPutHFileEntries(colFamily: String): Iterable[(ImmutableBytesWritable, KeyValue)] = {
    cells.flatMap(kv =>
      Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, kv.value.getBytes)))
    )
  }

  def toDeleteHFileEntries(colFamily: String): Iterable[(ImmutableBytesWritable, KeyValue)] = {
    val excludedColumns = Seq("p_ENT")
    if (key.contains("~LEU~")) {
      cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
        Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)))
      )
    } else {
      val cell = cells.head
      Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cell.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))
    }
  }

  def toDeletePeriodHFileEntries(colFamily: String): Iterable[(ImmutableBytesWritable, KeyValue)] =
    Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cells.head.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))

  def toDeleteHFile(colFamily: String): Iterable[(ImmutableBytesWritable, KeyValue)] = {
    val excludedColumns = Seq("p_ENT")

    if (key.contains("~LEU~")) {
      cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
        Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)))
      )
    } else {
      val cell = cells.head
      Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cell.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))
    }
  }

  def toDeleteHFileRows(colFamily: String): Iterable[(String, HFileCell)] = {
    val excludedColumns = Seq("p_ENT")

    if (key.contains("~LEU~")) {
      cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
        Seq((key, new HFileCell(key, colFamily, kv.column, "", HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn.ordinal())))
      )
    } else {
      val cell = cells.head //delete is made on row level, so there's no need to repeat delete for every column
      Seq((key, new HFileCell(key, colFamily, cell.column, "", HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn.ordinal())))
    }
  }

  def toDeleteColumnsExcept(colFamily: String, columns: Seq[String]): Iterable[KeyValue] = cells.filterNot(cell => columns.contains(cell.column)).map(kv =>

    new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)
  )

  def toPrintString: String = {
    val key = this.key
    val cellsToString = cells.map(cell => " \t" + cell.toPrintString).mkString("\n")

    '\n' +
      "key: " + key +
      '\n' +
      " cells: " +
      cellsToString
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