package model.domain

import global.{AppParams, Configs}
import model.hfile
import model.hfile.HFileCell
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HConstants, KeyValue}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.extensions.rdd.HBaseDataReader

import scala.util.Try

/**
  *
  */


case class HFileRow(key:String, cells:Iterable[KVCell[String,String]]) {

  def getLinkId = key.split("~").last

  def getCellValue(key: String, byKey: Boolean = true, default: String = "") = if (byKey) cells.collect { case KVCell(`key`, value) => if (value == null) default else value }.headOption.getOrElse(null)
  else cells.collect { case KVCell(value, `key`) => if (value == null) default else value }.headOption.getOrElse(null)

  def getCellArrayValue(key: String) = {

    val result = cells.collect { case KVCell(value, `key`) => value }
    if (result.isEmpty) null
    else result
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case HFileRow(otherKey, otherCells) if (
      (otherKey == this.key) && (this.cells.toSet == otherCells.toSet)
      ) => true
    case _ => false
  }

  //put type set as default
  def toHFileCells(columnFamily: String, kvType: Int = KeyValue.Type.Put.ordinal()) = cells.map(cell => HFileCell(key, columnFamily, cell.column, cell.value, kvType))

  def toEntRow = {
    import spark.extensions.sql._
    try {
      new GenericRowWithSchema(Array(

        getCellValue("ern"),
        Option(getCellValue("prn")).getOrElse(Configs.DEFAULT_PRN),
        getCellValue("entref"),
        getCellValue("name"),
        getCellValue("trading_style"),
        getCellValue("address1"),
        getCellValue("address2"),
        getCellValue("address3"),
        getCellValue("address4"),
        getCellValue("address5"),
        getCellValue("postcode"),
        getCellValue("sic07"),
        getCellValue("legal_status")
      ), entRowSchema)
    } catch {
        case e: java.lang.RuntimeException => {
          println(s"(toEntRow)Exception reading enterprise row with ern: ${getCellValue("ern")}")
          throw e
        }
    }
  }


  def toLeuRow = {
    import spark.extensions.sql._
    try {
      new GenericRowWithSchema(Array(

        getCellValue("ubrn"),
        getCellValue("ern"),
        getCellValue("crn"),
        getCellValue("name"),
        getCellValue("trading_style"),
        getCellValue("address1"),
        getCellValue("address2"),
        getCellValue("address3"),
        getCellValue("address4"),
        getCellValue("address5"),
        getCellValue("postcode"),
        getCellValue("sic07"),
        getCellValue("paye_jobs"),
        getCellValue("turnover"),
        getCellValue("legal_status"),
        getCellValue("trading_status"),
        getCellValue("birth_date"),
        getCellValue("death_date"),
        getCellValue("death_code"),
        getCellValue("uprn")
      ), leuRowSchema)
    } catch {
      case e: java.lang.RuntimeException => {
        println(s"(toLuRow)Exception reading enterprise row with ern: ${getCellValue("ern")}")
        throw e
      }
    }
  }

    def toLuRow = {
      import spark.extensions.sql._

      new GenericRowWithSchema(Array(
        getLinkId,
        getCellValue("p_ENT"),
        {
          val ch: String = getCellValue("CH", false)
          if (ch != null && ch.nonEmpty && ch.startsWith("c_")) {
            ch.substring(2)
          } else ch
        },
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
      ), luRowSchema)
    }

    def toUbrnErnRow = {
      import spark.extensions.sql._

      new GenericRowWithSchema(Array(
        getLinkId,
        getCellValue("p_ENT")
      ), existingLuBiRowSchema)
    }

    def toLouRow = {
      import spark.extensions.sql._

      try {
        new GenericRowWithSchema(Array(

          getCellValue("lurn"),
          getCellValue("luref"),
          getCellValue("ern"),
          getCellValue("name"),
          getCellValue("entref"),
          getCellValue("tradingstyle"),
          getCellValue("address1"),
          getCellValue("address2"),
          getCellValue("address3"),
          getCellValue("address4"),
          getCellValue("address5"),
          getCellValue("postcode"),
          getCellValue("sic07"),
          cells.find(_.column == "employees").map(_.value).getOrElse(null)
        ), louRowSchema)
      }catch {
        case e: java.lang.RuntimeException => {
          println(s"Exception reading enterprise row with ern: ${getCellValue("ern")}")
          throw e
        }
      }
    }

    def toHFileCellRow(colFamily: String): Iterable[(String, hfile.HFileCell)] = {
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


    def toDeleteHFileRows(colFamily: String): Iterable[(String, hfile.HFileCell)] = {
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

    def toPrintString = {
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

  def apply(entry: (String, Iterable[(String, String)])) = new HFileRow(entry._1, entry._2.map(c => KVCell(c)).toSeq)

  def apply(result: Result) = {
    val rowKey = Bytes.toString(result.getRow)
    val cells: Array[(String, String)] = result.rawCells().map(c => HBaseDataReader.getKeyValue(c)._2)
    new HFileRow(rowKey, cells.map(cell => KVCell(cell._1.trim(), cell._2.trim())))
  }

  implicit def buildFromHFileDataMap(entry: (String, Iterable[(String, String)])) = HFileRow(entry)
}