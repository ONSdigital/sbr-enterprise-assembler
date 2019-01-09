package model

case class KVCell[K, V](column: K, value: V) {

  def toPrintString: String = {
    val col = this.column
    val value = this.value
    val newLine = '\n'

    newLine +
      col.toString + ": " + value.toString
  }
}

object KVCell {
  def apply[T, V](entry: (T, V)): KVCell[T, V] = {
    new KVCell[T, V](entry._1, entry._2)
  }
}