package service



import dao.hbase.HBaseConnectionManager
import executors.RefreshClosures._
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseRefreshService extends HBaseConnectionManager with SparkSessionManager{

  def loadRefresh(appconf:AppParams) = withSpark{ implicit ss:SparkSession =>
    createDeleteLinksHFile(appconf)
    createLinksRefreshHFile(appconf)
    createEnterpriseRefreshHFile(appconf)
    withHbaseConnection{ implicit con:Connection => loadRefreshFromHFiles(appconf)}
  }

  def printDeleteData(appconf:AppParams) = withSpark{ implicit ss:SparkSession =>
    readDeleteData(appconf)
  }




}
