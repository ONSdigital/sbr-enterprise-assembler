package service

import closures.CreateNewPeriodClosure
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

        CreateNewPeriodClosure.addNewPeriodData(appconf)

        withHbaseConnection{implicit con:Connection =>
          println("Going to load hfile to hbase")

                                 HBaseDao.loadLinksHFile(con,appconf)
                                 HBaseDao.loadEnterprisesHFile(con,appconf)
                                 HBaseDao.loadLousHFile(con,appconf)
          println("hfile to hbase load done")
               }
  }

}
