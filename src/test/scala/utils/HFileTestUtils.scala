package utils

import global.AppParams
import model.domain._
import model.hfile.HFileCell
import org.apache.spark.rdd.RDD
/**
  *
  */
trait HFileTestUtils {




  def entToHFileCells(ents:RDD[HFileRow])(implicit configs: AppParams) = ents.flatMap(row =>
              row.cells.map(cell => HFileCell(row.key,configs.HBASE_ENTERPRISE_COLUMN_FAMILY,cell.column,cell.value)))


  def linksToHFileCells(ents:RDD[HFileRow])(implicit configs: AppParams) = ents.flatMap(row =>
              row.cells.map(cell => HFileCell(row.key,configs.HBASE_LINKS_COLUMN_FAMILY,cell.column,cell.value)))


  def localUnitsToHFileCells(ents:RDD[HFileRow])(implicit configs: AppParams) = ents.flatMap(row =>
              row.cells.map(cell => HFileCell(row.key,configs.HBASE_LOCALUNITS_COLUMN_FAMILY,cell.column,cell.value)))


}
