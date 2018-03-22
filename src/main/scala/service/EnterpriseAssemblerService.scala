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
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager{
  import global.Configs._



  def loadFromJson(appconf:AppParams){
    withSpark{ implicit ss:SparkSession =>
      ParquetDAO.jsonToParquet(PATH_TO_JSON)(ss,appconf)
      ParquetDAO.parquetToHFile(ss,appconf)
    }
    withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf)}
  }

  def readAll(appconf:AppParams) = withSpark{ implicit ss:SparkSession =>
    withHbaseConnection { implicit con: Connection =>
      HBaseDao.readAll(appconf,ss)
    }
  }


  def readWithFilter(appconf:AppParams) = withSpark{ implicit ss:SparkSession =>
    withHbaseConnection { implicit con: Connection =>
      HBaseDao.readWithFilterAll(appconf,ss)
    }
  }



  def readFromHBase(appconf:AppParams) = withHbaseConnection { implicit con: Connection =>
    HBaseDao.readLinksFromHbase(appconf)
  }

  def deleteFromHFile(appconf:AppParams) = withHbaseConnection { implicit con: Connection => HBaseDao.loadLinksHFile(con,appconf)}

  def loadFromParquet(appconf:AppParams){
    withSpark{ implicit ss:SparkSession => ParquetDAO.parquetToHFile(ss,appconf) }
    withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf) }
  }

  def refreshFromParquet(appconf:AppParams){
    withSpark{ implicit ss:SparkSession => ParquetDAO.toDeleteLinksHFile(ss,appconf) }
    withHbaseConnection { implicit con: Connection => HBaseDao.loadLinksHFile(con,appconf) }
  }

  def loadFromHFile(appconf:AppParams) = withHbaseConnection { implicit con: Connection => HBaseDao.loadHFiles(con,appconf)}

}
