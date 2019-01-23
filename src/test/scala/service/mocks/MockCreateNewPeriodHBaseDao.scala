package service.mocks

import dao.hbase.HBaseDao.{entsTableName, linksTableName}
import model.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import util.configuration.AssemblerConfiguration

object MockCreateNewPeriodHBaseDao  {

   def readTableWithKeyFilter(confs: Configuration, tableName: String, regex: String)
                                     (implicit spark: SparkSession): RDD[HFileRow] = {

    val res = tableName.split(":").last match {

      case AssemblerConfiguration.HBaseEnterpriseTableName => readEnterprisesWithKeyFilter(confs, regex)
      case AssemblerConfiguration.HBaseLinksTableName => readLinksWithKeyFilter(confs, regex)
      case _ => throw new IllegalArgumentException("invalid table name")

    }
    res
  }

  def adjustPathToExistingRecords(path: String): String = {
    val pathSeq = path.split("/")
    val res = pathSeq.init :+ "existing" :+ pathSeq.last
    res.mkString("/")

  }

  def readLinksWithKeyFilter(confs: Configuration, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(confs, linksTableName, regex)
  }

  def readEnterprisesWithKeyFilter(confs: Configuration, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(confs, entsTableName, regex)
  }

}


