package utils.data.consistency

import closures.mocks.{MockClosures, MockCreateNewPeriodHBaseDao}
import dao.hbase.HBaseConnectionManager
import dao.parquet.ParquetDao
import global.Configs.conf
import global.{AppParams, Configs}
import model.domain._
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest._
import spark.extensions.rdd.HBaseDataReader._
import utils.data.TestIds
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario
import utils.Paths

import scala.reflect.io.File
/**
  *
  */


class DataConsistencyCheck(val testDir:String) extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario{


}
