package service

import closures.AssembleUnitsClosure$
import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDao
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager
import util.options.ConfigOptions

trait AddNewPeriodDataService extends HBaseConnectionManager with SparkSessionManager {

  def createNewPeriodParquet(): Unit = withSpark {
    implicit ss: SparkSession => ParquetDao.jsonToParquet(ConfigOptions.PathToJSON)(ss)
  }

  def loadNewPeriodWithCalculationsData(): Unit = withSpark {
    implicit ss: SparkSession =>
      withHbaseConnection {
        implicit con: Connection =>

          AssembleUnitsClosure$.createUnitsHfiles

          HBaseDao.truncateTables
          HBaseDao.loadLinksHFile
          HBaseDao.loadEnterprisesHFile
          HBaseDao.loadLousHFile
          HBaseDao.loadLeusHFile
          HBaseDao.loadRusHFile
      }
  }
}
