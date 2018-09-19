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
    withHbaseConnection{implicit con:Connection =>
        NewPeriodClosure.createUnitsHfiles(appconf)

        implicit con:Connection =>
                                 HBaseDao.truncateTables(con,appconf)
                                 HBaseDao.loadHFiles(con,appconf)
        }}

  def loadNewPeriodWithCalculationsData(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession =>
    withHbaseConnection{implicit con:Connection =>

         ParquetDao.jsonToParquet(PATH_TO_JSON)(ss, appconf)
         RefreshPeriodWithCalculationsClosure.createUnitsHfiles(appconf)
         HBaseDao.truncateTables(con,appconf)
         HBaseDao.loadLinksHFile(con,appconf)
         HBaseDao.loadEnterprisesHFile(con,appconf)
         HBaseDao.loadLousHFile(con,appconf)
         HBaseDao.loadLeusHFile(con,appconf)
         HBaseDao.loadRusHFile(con,appconf)

               }
  }




}
