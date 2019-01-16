package service

import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import dao.spark.SparkSessionManager
import org.apache.hadoop.hbase.client.Connection
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import util.configuration.AssemblerConfiguration

object AddNewPeriodDataService {

  @transient lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")

  def createNewPeriodParquet(): Unit = SparkSessionManager.withSpark {
    implicit ss: SparkSession => ParquetDao.jsonToParquet(AssemblerConfiguration.BIFilePath)(ss)
  }

  def loadNewPeriodWithCalculationsData(): Unit = SparkSessionManager.withSpark {
    implicit ss: SparkSession =>
      HBaseConnectionManager.withHbaseConnection {
        implicit con: Connection =>

          AssembleUnits.createHfiles
          AssembleUnits.loadHFiles
      }
  }
}
