package closures.mocks

import closures.RefreshPeriodWithCalculationsClosure
import global.AppParams
import global.Configs.conf
import model.domain._
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.extensions.rdd.HBaseDataReader.readEntitiesFromHFile
import utils.data.consistency.DataConsistencyCheck

import scala.reflect.io.File

class NewPeriodConsistencyCheck extends DataConsistencyCheck("newperiod",RefreshPeriodWithCalculationsClosure){

  val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "LEU", "ons", "d", leuHfilePath,
      "ENT", "ons", "d",entHfilePath,
      "LOU", "ons", "d",louHfilePath,
      "REU", "ons", "d",ruHfilePath,
      parquetPath,
      "201804",payeFilePath,
      vatFilePath,
      "local",
      "add-calculated-period"
    )))

  override def beforeAll() = {
    implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    //val existingDF = readEntitiesFromHFile[HFileRow](existingRusRecordHFiles).collect
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    withHbaseConnection { implicit connection:Connection =>
      MockClosure.createUnitsHfiles(appConfs)(spark, connection)
    }
    spark.stop

  }
  override def afterAll() = {
    File(linkHfilePath).deleteRecursively()
    File(leuHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
    File(louHfilePath).deleteRecursively()
  }


  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val  ents = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList
      val  lous = readEntitiesFromHFile[LocalUnit](entHfilePath).collect.toList
      val  leus = readEntitiesFromHFile[LegalUnit](leuHfilePath).collect.toList
      val  rus = readEntitiesFromHFile[ReportingUnit](ruHfilePath).collect.toList
      val links: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList
      val res = checkIntegrity(ents, lous, leus, rus,links )
      res shouldBe true
      spark.stop()
    }
  }
}
