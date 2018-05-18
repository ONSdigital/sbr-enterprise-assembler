package model

import org.apache.hadoop.hbase.{HConstants, KeyValue}
import org.apache.hadoop.hbase.util.Bytes

/**
  *
  */
package object hfile {

  case class HFileCell(key:String, colFamily:String, qualifier:String, value:String, timestamp:Long, kvType:Int){
    def toKeyValue = {

      val deleteType = Seq(KeyValue.Type.DeleteFamily.ordinal(), KeyValue.Type.Delete.ordinal(), KeyValue.Type.DeleteColumn.ordinal(), KeyValue.Type.DeleteFamilyVersion.ordinal())
      if(deleteType.contains(kvType))  new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes,timestamp,KeyValue.Type.values().find(_.ordinal()==kvType).get)
      else new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, value.getBytes)

    }
    def toDeleteKeyValue = new KeyValue(key.getBytes, colFamily.getBytes, qualifier.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)
  }
  object HFileCell{
    def apply(kv:KeyValue) = new HFileCell(
                                            Bytes.toString(kv.getKey),
                                            Bytes.toString(kv.getFamilyArray),
                                            Bytes.toString(kv.getQualifierArray),
                                            Bytes.toString(kv.getValueArray),
                                            kv.getTimestamp,
                                            KeyValue.Type.codeToType(kv.getTypeByte).ordinal()
                                         )

    def apply(key:String, colFamily:String, qualifier:String, value:String) = new HFileCell(key, colFamily, qualifier, value,HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put.ordinal())
    //def apply(key:String, colFamily:String, qualifier:String, value:String, timestamp:Long, kvType:Int) = new HFileCell(key, colFamily, qualifier, value, timestamp, kvType)
    def apply(key:String, colFamily:String, qualifier:String, value:String, kvType:Int) = new HFileCell(key, colFamily, qualifier, value, HConstants.LATEST_TIMESTAMP, kvType)
    def apply(key:String, colFamily:String, qualifier:String, kvType:Int) = new HFileCell(key, colFamily, qualifier, "", HConstants.LATEST_TIMESTAMP, kvType)
  }

  case class Tables(enterprises: Seq[(String, HFileCell)], links:Seq[(String, HFileCell)], localUnits:Seq[(String, HFileCell)])
}
