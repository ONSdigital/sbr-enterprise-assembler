package model.domain

import model.hfile
import model.hfile.HFileCell
import org.apache.hadoop.hbase.{Cell, HConstants, KeyValue}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import spark.extensions.rdd.HBaseDataReader.getKeyValue

/**
  *
  */


case class HFileRow(key:String, cells:Iterable[KVCell[String,String]]){

  def getId = key.split("~").head

  override def equals(obj: scala.Any): Boolean = obj match{
    case HFileRow(otherKey, otherCells) if(
                  (otherKey == this.key) && (this.cells.toSet == otherCells.toSet)
                ) => true
    case _ => false
  }

  def toDeleteHFileEntries(colFamily:String): Iterable[(ImmutableBytesWritable, KeyValue)] = {
    val excludedColumns = Seq("p_ENT")
    if(key.contains("~LEU~")){ cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
      Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)))
    )}else{
    val cell = cells.head
    Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cell.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))
  }}


  def toDeleteHFileRows(colFamily:String): Iterable[(String, hfile.HFileCell)] = {
    val excludedColumns = Seq("p_ENT")
    if(key.contains("~LEU~")){ cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
      Seq((key, new HFileCell(key, colFamily, kv.column, "", HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn.ordinal())))
    )}else{
    val cell = cells.head  //delete is made on row level, so there's no need to repeat delete for every column
      Seq((key, new HFileCell(key, colFamily, cell.column, "", HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn.ordinal())))
  }}



  def toDeleteColumnsExcept(colFamily:String,columns:Seq[String]): Iterable[KeyValue] = cells.filterNot(cell => columns.contains(cell.column)).map(kv =>

    new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)
  )

  override def toString = {
    val key = this.key
    val cellsToString = cells.map(cell => " \t"+cell.toString).mkString("\n")

    '\n'+
    "key: " +key+
    '\n' +
    " cells: "+
    cellsToString
  }

}

object HFileRow{

  def getKeyValue[T <: Cell](kv:T): (String, (String, String)) =
    (Bytes.toString(kv.getRowArray).slice(kv.getRowOffset, kv.getRowOffset + kv.getRowLength),

      (Bytes.toString(kv.getQualifierArray).slice(kv.getQualifierOffset,
        kv.getQualifierOffset + kv.getQualifierLength),
        Bytes.toString(kv.getValueArray).slice(kv.getValueOffset,
          kv.getValueOffset + kv.getValueLength)))


  def apply(entry:(String, Iterable[(String, String)])) = new HFileRow(entry._1, entry._2.map(c => KVCell(c)).toSeq)
  def apply(result:Result) = {
    val rowKey = Bytes.toString(result.getRow)
    val cells: Array[(String, String)] = result.rawCells().map(c => getKeyValue(c)._2)
    new HFileRow(rowKey,cells.map(KVCell(_)))
  }

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = HFileRow(entry)
}