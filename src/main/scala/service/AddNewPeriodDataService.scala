package service

import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDao
import dao.spark.SparkSessionManager
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import util.configuration.AssemblerConfiguration

object AddNewPeriodDataService {

  def createNewPeriodParquet(): Unit = SparkSessionManager.withSpark {
    implicit ss: SparkSession => ParquetDao.jsonToParquet(AssemblerConfiguration.BIFilePath)(ss)
  }

  def loadNewPeriodWithCalculationsData(): Unit = SparkSessionManager.withSpark {
    implicit ss: SparkSession =>
      HBaseConnectionManager.withHbaseConnection {
        implicit con: Connection =>

          AssembleUnits.createUnitsHfiles

//          HBaseDao.truncateTables
//          HBaseDao.loadLinksHFile
//          HBaseDao.loadEnterprisesHFile
//          HBaseDao.loadLousHFile
//          HBaseDao.loadLeusHFile
//          HBaseDao.loadRusHFile
      }
  }
}
