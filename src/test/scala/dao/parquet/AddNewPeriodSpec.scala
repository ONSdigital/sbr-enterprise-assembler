package dao.parquet

import closures.CreateNewPeriodClosure
import dao.HFileTestUtils
import dao.hbase.HBaseDao
import global.AppParams
import model.domain.{Enterprise, HFileRow}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.reflect.io.File
/**
  *
  */

trait Paths{
  val jsonFilePath = "src/test/resources/data/newperiod/newPeriod.json"
  val linkHfilePath = "src/test/resources/data/newperiod/links"
  val entHfilePath = "src/test/resources/data/newperiod/enterprise"
  val louHfilePath = "src/test/resources/data/newperiod/lou"
  val parquetHfilePath = "src/test/resources/data/newperiod/sample.parquet"
  val payeFilePath = "src/test/resources/data/newperiod/newPeriodPaye.csv"
  val vatFilePath = "src/test/resources/data/newperiod/newPeriodVat.csv"
  val existingEntRecordHFiles = "src/test/resources/data/newperiod/existing/enterprise"
  val existingLinksRecordHFiles = "src/test/resources/data/newperiod/existing/links"
  val existingLousRecordHFiles = "src/test/resources/data/newperiod/existing/lou"
}

class AddNewPeriodSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestData with HFileTestUtils with Paths{
  import global.Configs._


  //val jsonFilePath = "src/test/resources/data/smallWithNullValues.json"


  val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "ENT", "ons", "d",entHfilePath,
      "LOU", "ons", "d",louHfilePath,
      parquetHfilePath,
      "201804",payeFilePath,
      vatFilePath,
      "local",
      "addperiod"
    )))

/*  def createExistingRecordsHFiles(implicit ss:SparkSession) = {
    val prevTimePeriod = {(appConfs.TIME_PERIOD.toInt - 1).toString}
    val ents: RDD[HFileRow] = HBaseDao.readEnterprisesWithKeyFilter(conf,appConfs,s"~$prevTimePeriod")
    val links: RDD[HFileRow] = HBaseDao.readLinksWithKeyFilter(conf,appConfs,s"~$prevTimePeriod")
    val lous: RDD[HFileRow] = HBaseDao.readLouWithKeyFilter(conf,appConfs,s".*~$prevTimePeriod~*.")

    ents.flatMap(_.toHFileCellRow(appConfs.HBASE_ENTERPRISE_COLUMN_FAMILY)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(existingEntRecordHFiles,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    links.flatMap(_.toHFileCellRow(appConfs.HBASE_LINKS_COLUMN_FAMILY)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(existingLinksRecordHFiles,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

    lous.flatMap(_.toHFileCellRow(appConfs.HBASE_LOCALUNITS_COLUMN_FAMILY)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(existingLousRecordHFiles,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],Configs.conf)

  }*/

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
    File(parquetHfilePath).deleteRecursively()
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
      val expected: List[Enterprise] = newPeriodEnts
      actual shouldBe expected
      spark.stop()

    }
  }



}

object MockHBaseDao extends HBaseDao with Paths{

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
}
