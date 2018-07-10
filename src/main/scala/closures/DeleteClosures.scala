package closures

import dao.hbase.HBaseDao
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession

trait DeleteClosures extends Serializable{

  def createDeleteDataForPeriodHfiles(appconf:AppParams)(implicit ss:SparkSession): Unit ={
    createDeleteLinksHFile(appconf)
    createDeleteEnterpriseHFile(appconf)
    createDeleteLouHFile(appconf)
  }

  def createDeleteLinksHFile(appconf:AppParams)(implicit ss:SparkSession){
    HBaseDao.saveDeletePeriodLinksToHFile(appconf)
  }
  def createDeleteEnterpriseHFile(appconf:AppParams)(implicit ss:SparkSession){
    HBaseDao.saveDeletePeriodEnterpriseToHFile(appconf)
  }

    def createDeleteLouHFile(appconf:AppParams)(implicit ss:SparkSession){
    HBaseDao.saveDeleteLouToHFile(appconf)
  }

}
object DeleteClosures extends DeleteClosures