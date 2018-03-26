package model.domain

import org.apache.hadoop.hbase.Cell

/**
  *
  */

case class KVCell[K,V](column:K, value:V)

object KVCell{
  def apply[T,V](entry:(T,V)) = {
    new KVCell[T,V](entry._1,entry._2)
  }
}