package dao.parquet

import closures.CreateNewPeriodClosure
import dao.hbase.HBaseDao
import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile

import scala.util.Random

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

  override def readLinksWithKeyFilter(confs: Configuration, appParams: AppParams, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_LINKS_HFILE)
    readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).filter(_.key.matches(regex))
  }


  override def readLouWithKeyFilter(confs: Configuration, appParams: AppParams, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_LOCALUNITS_HFILE)
    readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).filter(_.key.matches(regex))
  }


  override def readEnterprisesWithKeyFilter(confs: Configuration, appParams: AppParams, regex: String)(implicit spark: SparkSession): RDD[HFileRow] = {
    val path = adjustPathToExistingRecords(appParams.PATH_TO_ENTERPRISE_HFILE)
    readEntitiesFromHFile[HFileRow](path).sortBy(_.cells.map(_.column).mkString).filter(_.key.matches(regex))
}
}

object MockCreateNewPeriodClosure extends CreateNewPeriodClosure{
  override val hbaseDao = MockCreateNewPeriodHBaseDao
  override def generateUniqueKey = Random.alphanumeric.take(12).mkString + "TESTS" //to ensure letters and numbers present

}

