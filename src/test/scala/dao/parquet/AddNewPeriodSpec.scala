package dao.parquet

import closures.CreateNewPeriodClosure
import dao.HFileTestUtils
import dao.hbase.HBaseDao
import global.{AppParams, Configs}
import model.domain.{Enterprise, HFileRow, LocalUnit}
import model.hfile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.reflect.io.File
import scala.util.Random
/**
  *
  */


class AddNewPeriodSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestData with NewPeriodLinks with HFileTestUtils with ExistingEnts with ExistingLocalUnits with ExistingPeriodLinks{
  import global.Configs._

  lazy val testDir = "newperiod"

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
     createRecords(confs)(spark)
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
     val existingEnts = readEntitiesFromHFile[HFileRow](existingEntRecordHFiles).collect.toList.sortBy(_.key)
     val actualRows: Array[HFileRow] = readEntitiesFromHFile[HFileRow](entHfilePath).collect
     val actual = actualRows.map(Enterprise(_))
     val actualEnts = actual.map(ent => {
       if(ent.ern.endsWith("TESTS")) ent.copy(ern=newEntErn)
       else ent}).toList.sortBy(_.ern)
     val expected: List[Enterprise] = newPeriodEnts.sortBy(_.ern)
     actualEnts shouldBe expected
     spark.stop()

   }
 }


 "assembler" should {
   "create hfiles populated with expected local units data" in {

     implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
     val hasLettersAndNumbersRegex = "^.*(?=.{4,10})(?=.*\\d)(?=.*[a-zA-Z]).*$"
     val existing = readEntitiesFromHFile[HFileRow](existingLousRecordHFiles).collect.toList.sortBy(_.key)
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

def saveToHFile(rows:Seq[HFileRow], colFamily:String, appconf:AppParams, path:String)(implicit spark:SparkSession) = {
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
     val records: RDD[HFileRow] = spark.sparkContext.parallelize(rows)
     val cells: RDD[(String, hfile.HFileCell)] = records.flatMap(_.toHFileCellRow(colFamily))
     cells.sortBy(t => s"${t._2.key}${t._2.qualifier}")
       .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
       .saveAsNewAPIHadoopFile(path,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)
}

  def createRecords(appconf:AppParams)(implicit spark:SparkSession) = {
    saveToHFile(ents,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingEntRecordHFiles)
    saveToHFile(links,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
    saveToHFile(existingLous,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
  }

}


