package model.domain

/**
  *
  */

case class HFileCell[K,V](column:K,value:V)

object HFileCell{
  def apply[T,V](entry:(T,V)) = {
    new HFileCell[T,V](entry._1,entry._2)
  }
}