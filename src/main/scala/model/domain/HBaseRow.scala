package model.domain

import org.apache.hadoop.hbase.{Cell, HConstants, KeyValue}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import spark.extensions.rdd.HBaseDataReader.getKeyValue

/**
  *
  */


case class HBaseRow(key:String, cells:Iterable[HBaseCell[String,String]]){

  override def equals(obj: scala.Any): Boolean = obj match{
    case HBaseRow(otherKey, otherCells) if(
                  (otherKey == this.key) && (this.cells.toSet == otherCells.toSet)
                ) => true
    case _ => false
  }

/*  def toDeleteHBaseRow(colFamily:String): Iterable[(ImmutableBytesWritable, KeyValue)] = cells.map(cell =>
    (new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cell.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))*/


  def toDeleteHBaseRows(colFamily:String): Iterable[(ImmutableBytesWritable, KeyValue)] = {
    val excludedColumns = Seq("p_ENT")
    if(key.contains("~LEU~")){ cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
      Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)))
    )}else{
    val cell = cells.head
    Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cell.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))
  }}

  def toDeleteColumnsExcept(colFamily:String,columns:Seq[String]): Iterable[KeyValue] = { cells.filterNot(cell => columns.contains(cell.column)).map(kv =>

    new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)
  )}

}

object HBaseRow{

  def getKeyValue[T <: Cell](kv:T): (String, (String, String)) =
    (Bytes.toString(kv.getRowArray).slice(kv.getRowOffset, kv.getRowOffset + kv.getRowLength),

      (Bytes.toString(kv.getQualifierArray).slice(kv.getQualifierOffset,
        kv.getQualifierOffset + kv.getQualifierLength),
        Bytes.toString(kv.getValueArray).slice(kv.getValueOffset,
          kv.getValueOffset + kv.getValueLength)))


  def apply(entry:(String, Iterable[(String, String)])) = new HBaseRow(entry._1, entry._2.map(c => HBaseCell(c)).toSeq)
  def apply(result:Result) = {
    val rowKey = Bytes.toString(result.getRow)
    val cells: Array[(String, String)] = result.rawCells().map(c => getKeyValue(c)._2)
    new HBaseRow(rowKey,cells.map(HBaseCell(_)))
  }



  /*def apply(entry:(String, Iterable[Cell])) = new HBaseRow(
    entry._1,
    entry._2.map(c => HBaseCell(getKeyValue(c)._2)))*/
  //def apply(entry:(String, Array[(String, String)])) = new HBaseRow(entry._1, entry._2.map(c => HBaseCell(c)))

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = HBaseRow(entry)
}