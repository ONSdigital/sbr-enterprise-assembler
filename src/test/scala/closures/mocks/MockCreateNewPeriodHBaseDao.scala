package closures.mocks

import dao.hbase.HBaseDao
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import util.options.ConfigOptions

object MockCreateNewPeriodHBaseDao extends HBaseDao {

  override def readTableWithKeyFilter(confs: Configuration, tableName: String, regex: String)
                                     (implicit spark: SparkSession): RDD[HFileRow] = {

    val res = tableName.split(":").last match {

      case ConfigOptions.HBaseEnterpriseTableName => readEnterprisesWithKeyFilter(confs, regex)
      case ConfigOptions.HBaseLinksTableName => readLinksWithKeyFilter(confs, regex)
      case ConfigOptions.HBaseLocalUnitsTableName => readLouWithKeyFilter(confs, regex)
      case _ => throw new IllegalArgumentException("invalid table name")

    }
    res
  }

  def adjustPathToExistingRecords(path: String): String = {
    val pathSeq = path.split("/")
    val res = pathSeq.init :+ "existing" :+ pathSeq.last
    res.mkString("/")

  }

}



