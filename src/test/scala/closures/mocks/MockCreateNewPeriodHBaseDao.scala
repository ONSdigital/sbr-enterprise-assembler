package closures.mocks

import dao.hbase.HBaseDao
import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MockCreateNewPeriodHBaseDao extends HBaseDao {

  override def readTableWithKeyFilter(confs: Configuration, tableName: String, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {

    val res = tableName.split(":").last match {

      case AppParams.HBASE_ENTERPRISE_TABLE_NAME => readEnterprisesWithKeyFilter(confs, regex)
      case AppParams.HBASE_LINKS_TABLE_NAME => readLinksWithKeyFilter(confs, regex)
      case AppParams.HBASE_LOCALUNITS_TABLE_NAME => readLouWithKeyFilter(confs, regex)
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



