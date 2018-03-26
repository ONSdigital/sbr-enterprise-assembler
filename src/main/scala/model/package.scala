package model

import org.apache.hadoop.hbase.{HConstants, KeyValue}
import org.apache.hadoop.hbase.util.Bytes

/**
  *
  */
package object hfile {

  case class HFileCell(key:String, colFamily:String, qualifier:String, value:String){
    def toKeyValue = new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, value.getBytes)
    def toDeleteKeyValue = new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)
  }
  object HFileCell{
    def apply(kv:KeyValue) = new HFileCell(
      Bytes.toString(kv.getKey),Bytes.toString(kv.getFamilyArray),Bytes.toString(kv.getQualifierArray),Bytes.toString(kv.getValueArray)
    )
  }

  case class Tables(enterprises: Seq[(String, HFileCell)], links:Seq[(String, HFileCell)])
}
