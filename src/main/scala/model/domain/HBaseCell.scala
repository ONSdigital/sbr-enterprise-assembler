package model.domain

import org.apache.hadoop.hbase.Cell

/**
  *
  */

case class HBaseCell[K,V](column:K, value:V)

object HBaseCell{
  def apply[T,V](entry:(T,V)) = {
    new HBaseCell[T,V](entry._1,entry._2)
  }
}