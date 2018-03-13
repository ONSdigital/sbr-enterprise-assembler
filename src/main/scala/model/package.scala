package model

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes

/**
  *
  */
package object hfile {

  case class RowObject(key:String, colFamily:String, qualifier:String, value:String){
    def toKeyValue = new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, value.getBytes)
  }
  object RowObject{
    def apply(kv:KeyValue) = new RowObject(
      Bytes.toString(kv.getKey),Bytes.toString(kv.getFamilyArray),Bytes.toString(kv.getQualifierArray),Bytes.toString(kv.getValueArray)
    )
  }

  case class Tables(enterprises: Seq[(String, RowObject)],links:Seq[(String, RowObject)])
}
