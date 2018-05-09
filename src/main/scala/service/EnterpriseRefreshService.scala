package service



import dao.hbase.HBaseConnectionManager
import closures.RefreshClosures
import dao.parquet.ParquetDAO
import global.AppParams
import global.Configs.PATH_TO_JSON
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseRefreshService extends HBaseConnectionManager with SparkSessionManager with RefreshClosures{

  def createRefreshParquet(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession => ParquetDAO.jsonToParquet(PATH_TO_JSON)(ss, appconf)}

  def loadRefreshFromParquet(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession => withHbaseConnection{ implicit con:Connection =>
       createDeleteLinksHFile(appconf)
       createLinksRefreshHFile(appconf)
       createEnterpriseRefreshHFile(appconf)
       loadRefreshFromHFiles(appconf)
    }
  }

  def printDeleteData(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession =>
    withHbaseConnection{ implicit con:Connection =>
      readDeleteData(appconf)
    }
  }




}
