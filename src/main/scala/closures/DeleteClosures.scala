package closures

import dao.hbase.HBaseDao
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession

object DeleteClosures {

  def deleteDataForPeriod(appconf:AppParams)(implicit ss:SparkSession,connection:Connection): Unit ={
    createDeleteLinksHFile(appconf)
    createDeleteEnterpriseHFile(appconf)
    createDeleteLouHFile(appconf)
  }

  def createDeleteLinksHFile(appconf:AppParams)(implicit ss:SparkSession,connection:Connection){
    HBaseDao.saveDeletePeriodLinksToHFile(appconf)
  }
  def createDeleteEnterpriseHFile(appconf:AppParams)(implicit ss:SparkSession,connection:Connection){
    HBaseDao.saveDeletePeriodEnterpriseToHFile(appconf)
  }

    def createDeleteLouHFile(appconf:AppParams)(implicit ss:SparkSession,connection:Connection){
    HBaseDao.saveDeleteLouToHFile(appconf)
  }



}
