package service

import closures.DeleteClosures
import dao.hbase.{HBaseConnectionManager, HBaseDao}
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

trait DeleteDataService extends HBaseConnectionManager with SparkSessionManager{

  def deletePeriod(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession =>
    withHbaseConnection{implicit con:Connection =>
      DeleteClosures.deleteDataForPeriod(appconf)
      HBaseDao.loadHFiles(con,appconf)
    }
  }
}
