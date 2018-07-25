package service

import closures.NewPeriodClosure
import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDao
import global.AppParams
import global.Configs.PATH_TO_JSON
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager
import closures._

trait AdminCalculationService extends HBaseConnectionManager with SparkSessionManager{


  def addCalculations(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession =>

    CalculateClosure.calculate(appconf)

    withHbaseConnection{implicit con:Connection =>

      HBaseDao.loadEnterprisesHFile(con,appconf)
      HBaseDao.loadLousHFile(con,appconf)

    }
  }
}
