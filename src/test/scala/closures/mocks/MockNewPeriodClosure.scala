package closures.mocks

import closures.NewPeriodClosure
import global.AppParams
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import spark.extensions.sql._
import utils.Paths
import utils.data.expected.ExpectedDataForAddNewPeriodScenario

/**
  *
  */
trait MockNewPeriodClosure extends NewPeriodClosure with MockClosures with ExpectedDataForAddNewPeriodScenario with Paths{this: { val testDir: String } =>

    override val hbaseDao = MockCreateNewPeriodHBaseDao

}



