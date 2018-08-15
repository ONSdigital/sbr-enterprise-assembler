package closures

import closures.mocks.{MockClosures, MockCreateNewPeriodHBaseDao}
import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDao
import global.{AppParams, Configs}
import model.domain.{HFileRow, LocalUnit}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.calculations.AdminDataCalculator
import spark.extensions.rdd.HBaseDataReader._
import utils.{HFileTestUtils, Paths}
import utils.data.existing.ExistingData
import utils.data.expected.ExpectedDataForAddNewPeriodScenario

import scala.reflect.io.File

class AddNewPeriodWithMissingLouSpec extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with HFileTestUtils{
  import global.Configs._

  lazy val testDir = "missinglou"

  object MockRefreshPeriodWithCalculationsClosure$ extends RefreshPeriodWithCalculationsClosure with MockClosures {

    override val hbaseDao: HBaseDao = MockCreateNewPeriodHBaseDao

    override val ernMapping: Map[String, String] = Map(
      ("NEW ENTERPRISE LU" -> newEntErn),
      ("INDUSTRIES LTD" -> entWithMissingLouId)
    )

    override val lurnMapping: Map[String, String] = Map(
      ("NEW ENTERPRISE LU" ->  newLouLurn),
      ("INDUSTRIES LTD" ->  missingLouLurn)
    )
  }

  val appConfs = AppParams((Array[String](
                            "LINKS", "ons", "l", linkHfilePath,
                            "ENT", "ons", "d",entHfilePath,
                            "LOU", "ons", "d",louHfilePath,
                            parquetPath,
                            "201804",payeFilePath,
                            vatFilePath,
                            "local",
                            "addperiod"
                            )))

  override def beforeAll() = {

        val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val confs = appConfs
        createRecords(confs)(spark)
        ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
    withHbaseConnection { implicit connection: Connection =>
      MockRefreshPeriodWithCalculationsClosure$.createHFilesWithRefreshPeriodDataWithCalculations(appConfs)(spark, connection)
    }
        spark.stop()
  }

  override def afterAll() = {
        File(parquetPath).deleteRecursively()
        File(linkHfilePath).deleteRecursively()
        File(entHfilePath).deleteRecursively()
        File(louHfilePath).deleteRecursively()
        File(existingRecordsDir).deleteRecursively()
  }



"assembler" should {
   "create hfiles populated with expected local units data" in {

        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val actual: List[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.toList.sortBy(_.lurn)
        val expected: List[LocalUnit] = newPeriodWithMissingLocalUnit.sortBy(_.lurn).sortBy(_.lurn)
        actual shouldBe expected
        spark.stop()
   }
}

"assembler" should {
    "create hfiles populated with expected links data" in {

        implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
        val confs = appConfs
        val existing = readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).collect.toList.sortBy(_.key)
        val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(_.key)
        val expected = newPeriodLinks.sortBy(_.key)
        actual shouldBe expected
        spark.stop()

        }
}


def saveToHFile(rows:Seq[HFileRow], colFamily:String, appconf:AppParams, path:String)(implicit spark:SparkSession) = {
    val records: RDD[HFileRow] = spark.sparkContext.parallelize(rows)
    val cells: RDD[(String, hfile.HFileCell)] = records.flatMap(_.toHFileCellRow(colFamily))
    cells.sortBy(t => s"${t._2.key}${t._2.qualifier}")
    .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
    .saveAsNewAPIHadoopFile(path,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)
}

def createRecords(appconf:AppParams)(implicit spark:SparkSession) = {
    saveToHFile(ents,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingEntRecordHFiles)
    saveToHFile(existingLinksForMissingLousScenario,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
    saveToHFile(louForLouMissingScenario,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
}


}

