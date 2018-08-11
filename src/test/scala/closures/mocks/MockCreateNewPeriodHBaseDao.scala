package closures.mocks

import dao.hbase.HBaseDao
import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql._

object MockCreateNewPeriodHBaseDao extends HBaseDao {



  override def readTableWithKeyFilter(confs: Configuration, appParams: AppParams, tableName: String, regex: String)(implicit spark: SparkSession) = {

    val res = tableName.split(":").last match {

      case appParams.HBASE_ENTERPRISE_TABLE_NAME => readEnterprisesWithKeyFilter(confs, appParams, regex)
      case appParams.HBASE_LINKS_TABLE_NAME => readLinksWithKeyFilter(confs, appParams, regex)
      case appParams.HBASE_LOCALUNITS_TABLE_NAME => readLouWithKeyFilter(confs, appParams, regex)
      case _ => throw new IllegalArgumentException("invalid table name")

    }
    res
  }

  def adjustPathToExistingRecords(path: String) = {
    val pathSeq = path.split("/")
    val res = pathSeq.init :+ "existing" :+ pathSeq.last
    res.mkString("/")

  }

}



