package service

import closures.{NewPeriodClosure, RefreshPeriodWithCalculationsClosure}
import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDao
import global.AppParams
import global.Configs.PATH_TO_JSON
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

trait AddNewPeriodDataService extends HBaseConnectionManager with SparkSessionManager{

  def createNewPeriodParquet(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession => ParquetDao.jsonToParquet(PATH_TO_JSON)(ss, appconf)}

  def loadNewPeriodData(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession =>

        NewPeriodClosure.addNewPeriodData(appconf)
        HBaseDao.createCleanupTablesHFiles(appconf,global.Configs.conf)

        withHbaseConnection{implicit con:Connection =>
                                 HBaseDao.loadDeleteHFiles(con,appconf)
                                 HBaseDao.loadHFiles(con,appconf)
        }}

    def loadNewPeriodWithCalculationsData(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession =>
        //ParquetDao.jsonToParquet(PATH_TO_JSON)(ss, appconf)
        RefreshPeriodWithCalculationsClosure.createHFilesWithRefreshPeriodDataWithCalculations(appconf)
        HBaseDao.createCleanupTablesHFiles(appconf,global.Configs.conf)

        withHbaseConnection{implicit con:Connection =>
                                 HBaseDao.loadDeleteHFiles(con,appconf)
                                 HBaseDao.loadLinksHFile(con,appconf)
                                 HBaseDao.loadEnterprisesHFile(con,appconf)
                                 HBaseDao.loadLousHFile(con,appconf)

               }
  }




}
