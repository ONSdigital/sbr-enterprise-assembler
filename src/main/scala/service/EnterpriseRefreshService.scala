package service



import closures.RefreshPeriodWithCalculationsClosure
import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import global.AppParams
import global.Configs.PATH_TO_JSON
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseRefreshService extends HBaseConnectionManager with SparkSessionManager with RefreshPeriodWithCalculationsClosure{

  def refresh(appconf:AppParams) = withSpark(appconf){ implicit ss:SparkSession =>
    ParquetDao.jsonToParquet(PATH_TO_JSON)(ss, appconf)
    refreshPeriodDataWithCalculations(appconf)

  }


}
