package dao.parquet

import closures.CreateNewPeriodClosure
import dao.HFileTestUtils
import dao.hbase.HBaseDao
import global.AppParams
import model.domain.{Enterprise, HFileRow, LocalUnit}
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


class AddNewPeriodSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestData with NewPeriodLinks with HFileTestUtils with AddPeriodPaths{
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
     //ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
     MockCreateNewPeriodClosure.addNewPeriodData(appConfs)(spark)
     spark.stop()


 }

 override def afterAll() = {
    //File(parquetPath).deleteRecursively()
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
     //val existing = readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).collect.toList.sortBy(_.key)
     val actual: List[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHfilePath).collect.map(lou => {
       if(lou.ern.endsWith("TESTS")) lou.copy(lurn = newLouLurn, ern = newEntErn)
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

     //val existing = readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).collect.toList.sortBy(_.key)

     val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(_.key)
     /**
       * substitute system generated key with const values for comparison*/
     val actualUpdated: Seq[HFileRow] = actual.map(f = row => {
       if (row.key.contains(s"~ENT~${confs.TIME_PERIOD}") && row.key.split("~").head.endsWith("TESTS")) {
         row.copy(key = s"$newEntErn~ENT~${confs.TIME_PERIOD}", cells = row.cells.map(cell => if (cell.value == "LOU") cell.copy(column = s"c_$newLouLurn") else cell).toList.sortBy(_.column))
       }
       else if (row.key.contains(s"~LOU~${confs.TIME_PERIOD}") && row.key.split("~").head.endsWith("TESTS")) {
         row.copy(key = s"$newLouLurn~LOU~${confs.TIME_PERIOD}", cells = row.cells.map(cell => if (cell.column == "p_ENT") cell.copy(value = s"$newEntErn") else cell).toList.sortBy(_.column))
       }
       else row.copy(cells = row.cells.map(cell => if (cell.value.endsWith("TESTS")) cell.copy(value = newEntErn) else cell).toList.sortBy(_.column))
     }).sortBy(_.key)
     val expected = newPeriodLinks//.sortBy(_.key)
     actualUpdated shouldBe expected


     spark.close()

   }
 }



}

object MockHBaseDao extends HBaseDao with AddPeriodPaths{

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
 override def generateUniqueKey = Random.alphanumeric.take(12).mkString + "TESTS" //to ensure letters and numbers present

}

trait AddPeriodPaths{
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