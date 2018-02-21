import org.apache.hadoop.hbase.KeyValue

/**
  *
  */
package object model {

  case class RowObject(key:String, colFamily:String, qualifier:String, value:String){
    def toKeyValue = new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, value.getBytes)
  }

  case class Tables(enterprises: Seq[(String, RowObject)],links:Seq[(String, RowObject)])
}
