package service



import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDAO
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseRefreshService extends HBaseConnectionManager with SparkSessionManager{

  import global.Configs._

  def loadRefresh(appconf:AppParams) = withSpark{ implicit ss:SparkSession =>
    createDeleteLinksHFile(appconf)
    ParquetDAO.createRefreshLinksHFile(appconf)
    ParquetDAO.createEnterpriseRefreshHFile(appconf)
    loadRefreshFromHFiles(appconf)
  }


  def createDeleteLinksHFile(appconf:AppParams)(implicit ss:SparkSession){
    val regex = ".*(?<!~ENT~"+{appconf.TIME_PERIOD}+")$"
    val localConfCopy = conf
    HBaseDao.saveDeleteLinksToHFile(localConfCopy,appconf,regex)
  }

  def loadRefreshFromHFiles(appconf:AppParams) =  withHbaseConnection { implicit con: Connection =>

    HBaseDao.loadDeleteLinksHFile(con,appconf)
    HBaseDao.loadRefreshLinksHFile(con,appconf)
    HBaseDao.loadEnterprisesHFile(con,appconf)

  }
}
