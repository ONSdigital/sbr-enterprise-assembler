package dao.parquet

import closures.CreateNewPeriodClosure
import dao.HFileTestUtils
import dao.hbase.HBaseDao
import global.AppParams
import model.domain.{Enterprise, HFileRow, LocalUnit}
import model.hfile.HFileCell
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.reflect.io.File
import scala.util.Random
/**
  *
  */


class AddNewPeriodSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestData with NewPeriodLinks with HFileTestUtils with Const{
  import global.Configs._



  val appConfs = AppParams(
    (Array[String](
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
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //HBaseDao.copyExistingRecordsToHFiles(appConfs)(spark)
    ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
    MockCreateNewPeriodClosure.addNewPeriodData(appConfs)(spark)
    spark.stop()


  }

  override def afterAll() = {
    File(parquetPath).deleteRecursively()
    File(linkHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
    File(louHfilePath).deleteRecursively()
  }


  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val hasLettersAndNumbersRegex ="^.*(?=.{4,10})(?=.*\\d)(?=.*[a-zA-Z]).*$"
      val actual: List[Enterprise] = readEntitiesFromHFile[Enterprise](entHfilePath).collect.map(ent => {
        if(ent.ern.matches(hasLettersAndNumbersRegex)) ent.copy(ern=newEntErn)
        else ent}).toList.sortBy(_.ern)
      val expected: List[Enterprise] = newPeriodEnts.sortBy(_.ern)
      actual shouldBe expected
      spark.stop()

    }
  }

  "assembler" should {
    "create hfiles populated with expected local units data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      val hasLettersAndNumbersRegex = "^.*(?=.{4,10})(?=.*\\d)(?=.*[a-zA-Z]).*$"
      val actual: List[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.map(lou => {
        if(lou.ern.matches(hasLettersAndNumbersRegex)) lou.copy(lurn = newLouLurn, ern = newEntErn)
        else lou}).toList.sortBy(_.lurn)
      val expected: List[LocalUnit] = newPeriodLocalUnits
      actual shouldBe expected
      spark.stop()

    }
  }


  "assembler" should {
    "create hfiles populated with expected links data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
      val confs = appConfs
      //ParquetDao.parquetCreateNewToHFile(spark,appConfs)

      val systemGeneratedErnBasedENTKeyRegex = "(?!^[0-9]*$)(?!^[a-zA-Z]*$)^([a-zA-Z0-9]{6,50}).~ENT~"+confs.TIME_PERIOD+"$"
      val systemGeneratedLurnBasedLOUKeyRegex = "(?!^[0-9]*$)(?!^[a-zA-Z]*$)^([a-zA-Z0-9]{6,50}).~LOU~"+confs.TIME_PERIOD+"$"
      val systemGeneratedKeyRegex = "(?!^[0-9]*$)(?!^[a-zA-Z]*$)^([a-zA-Z0-9]{6,50})$"

      val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(_.cells.map(_.column).mkString)


      /* HFileRow-s need to be flatMapped to HFileCell-s to avoid ordering mismatch on HFileRow.cells
      *  and system generated ern-s and lurn-s replaced with static values to enable result matching
      * */
      val actualUpdated = actual.flatMap(row => {
        row.toHFileCells(confs.HBASE_ENTERPRISE_COLUMN_FAMILY).map(cell => cell match{

          case cell@HFileCell(key, _, _, "LOU", _, _) if (key.matches(systemGeneratedErnBasedENTKeyRegex)) => cell.copy(key = s"$newEntErn~ENT~${confs.TIME_PERIOD}", qualifier = s"c_$newLouLurn")
          case cell@HFileCell(key, _, _, _, _, _) if (key.matches(systemGeneratedErnBasedENTKeyRegex)) => cell.copy(key = s"$newEntErn~ENT~${confs.TIME_PERIOD}")
          case cell@HFileCell(key, _, "p_ENT", value, _, _) if (key.matches(systemGeneratedLurnBasedLOUKeyRegex)) => cell.copy(key = s"$newLouLurn~LOU~${confs.TIME_PERIOD}", value = newEntErn)
          case cell@HFileCell(_, _, "p_ENT", value, _, _) if (value.matches(systemGeneratedKeyRegex)) => cell.copy(value = newEntErn)
          case cell => cell

        })
      }).toSet
      val expected = newPeriodLinks.toSet
      actualUpdated shouldBe expected


      spark.close()

    }
  }



}

object MockHBaseDao extends HBaseDao with Const{

  override def readTableWithKeyFilter(confs:Configuration,appParams:AppParams, tableName:String, regex:String)(implicit spark:SparkSession) = {

    val res = tableName.split(":").last match{

      case appParams.HBASE_ENTERPRISE_TABLE_NAME => readEnterprisesWithKeyFilter(confs,appParams,regex)
      case appParams.HBASE_LINKS_TABLE_NAME => readLinksWithKeyFilter(confs,appParams,regex)
      case appParams.HBASE_LOCALUNITS_TABLE_NAME => readLouWithKeyFilter(confs,appParams,regex)
      case _ => throw new IllegalArgumentException("invalid table name")

    }
    res
  }

  override def readLinksWithKeyFilter(confs:Configuration, appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] =
    readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).sortBy(_.cells.map(_.column).mkString).filter(_.key.matches(regex))


  override def readLouWithKeyFilter(confs:Configuration,appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] =
    readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).sortBy(_.cells.map(_.column).mkString).filter(_.key.matches(regex))


  override def readEnterprisesWithKeyFilter(confs:Configuration,appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] =
    readEntitiesFromHFile[HFileRow](existingEntRecordHFiles).sortBy(_.cells.map(_.column).mkString).filter(_.key.matches(regex))
}

object MockCreateNewPeriodClosure extends CreateNewPeriodClosure{
  override val hbaseDao = MockHBaseDao
  override def generateUniqueKey = Random.alphanumeric.take(16).mkString + "1a" //to ensure letters and numbers present

}

trait Const{
  val jsonFilePath = "src/test/resources/data/newperiod/newPeriod.json"
  val linkHfilePath = "src/test/resources/data/newperiod/links"
  val entHfilePath = "src/test/resources/data/newperiod/enterprise"
  val louHfilePath = "src/test/resources/data/newperiod/lou"
  val parquetPath = "src/test/resources/data/newperiod/sample.parquet"
  val payeFilePath = "src/test/resources/data/newperiod/newPeriodPaye.csv"
  val vatFilePath = "src/test/resources/data/newperiod/newPeriodVat.csv"
  val existingEntRecordHFiles = "src/test/resources/data/newperiod/existing/enterprise"
  val existingLinksRecordHFiles = "src/test/resources/data/newperiod/existing/links"
  val existingLousRecordHFiles = "src/test/resources/data/newperiod/existing/lou"
}