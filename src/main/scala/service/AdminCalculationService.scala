package service

import closures._
import dao.hbase.{HBaseConnectionManager, HBaseDao}
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

trait AdminCalculationService extends HBaseConnectionManager with SparkSessionManager{


  def addCalculations(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession =>

    CalculateClosure.updateCalculations(appconf)

    withHbaseConnection{implicit con:Connection =>

      HBaseDao.loadEnterprisesHFile(con,appconf)
      HBaseDao.loadLousHFile(con,appconf)

    }
  }
}