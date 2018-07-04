package closures

import closures.mocks.MockCreateNewPeriodClosure
import dao.parquet.ParquetDao
import test.data.existing.ExistingData
import test.data.expected.ExpectedDataForAddNewPeriodScenario
import global.{AppParams, Configs}
import model.domain.{HFileRow, LocalUnit}
import model.hfile
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._
import test.Paths
import test.utils.HFileTestUtils

import scala.reflect.io.File

class AddNewPeriodWithMissingLouSpec extends Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with ExpectedDataForAddNewPeriodScenario with HFileTestUtils{
  import global.Configs._

 lazy val testDir = "missinglou"

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
        ParquetDao.jsonToParquet(jsonFilePath)(spark, confs)
        MockCreateNewPeriodClosure.addNewPeriodData(appConfs)(spark)
        spark.stop()
  }

/*override def afterAll() = {
      File(parquetPath).deleteRecursively()
      File(linkHfilePath).deleteRecursively()
      File(entHfilePath).deleteRecursively()
      File(louHfilePath).deleteRecursively()
      File(existingRecordsDir).deleteRecursively()
}*/



"assembler" should {
"create hfiles populated with expected local units data" in {

    implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    val hasLettersAndNumbersRegex = "^.*(?=.{4,10})(?=.*\\d)(?=.*[a-zA-Z]).*$"

    val updatedLous = readEntitiesFromHFile[HFileRow](louHfilePath).collect.toList.sortBy(_.key)

    val data = readEntitiesFromHFile[LocalUnit](louHfilePath).collect
    val actualCorrected: Set[LocalUnit] = data.map(lou => {
                                                    if (lou.name=="INDUSTRIES LTD" && lou.lurn.endsWith("TESTS"))  lou.copy(lurn = missingLouLurn)
                                                    else if(lou.ern.endsWith("TESTS")) lou.copy(lurn = newLouLurn, ern = newEntErn)
                                                    else lou
                                                  }
                                          ).toSet
    val expected: Set[LocalUnit] = newPeriodWithMissingLocalUnit.sortBy(_.lurn).toSet
    actualCorrected shouldBe expected
    spark.stop()

}
}


"assembler" should {
"create hfiles populated with expected links data" in {

implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
  val confs = appConfs

  val existing = readEntitiesFromHFile[HFileRow](existingLinksRecordHFiles).collect.toList.sortBy(_.key)

  val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(_.key)
  /**
  * substitute system generated key with const values for comparison*/
  val actualUpdated: Seq[HFileRow] = replaceIds(actual,confs).sortBy(_.key)
  val expected = newPeriodLinks.sortBy(_.key)
  actualUpdated shouldBe expected

  spark.close()

}
}

def replaceIds(rows:Seq[HFileRow],appConfs:AppParams) = {

  val withNewEntRowUpdated = rows.map(row => {
                //replace id in ENTS
                if (row.key.endsWith(s"TESTS~ENT~${appConfs.TIME_PERIOD}")) {
                  row.copy(key = s"$newEntErn~ENT~${appConfs.TIME_PERIOD}", cells = row.cells.map(cell => if (cell.value == "LOU") cell.copy(column = s"c_$newLouLurn") else cell).toList.sortBy(_.column))
                  //replace id in LEU
                }else if (row.key.contains(s"~LEU~${appConfs.TIME_PERIOD}") && row.cells.find(cell => cell.column=="p_ENT" && cell.value.endsWith("TESTS")).isDefined) {
                  row.copy(cells = row.cells.map(cell => if (cell.column == "p_ENT") cell.copy(value = newEntErn)
                  else cell
                  ).toList.sortBy(_.column))
                  //replace id in LOU
                }else if(row.key.endsWith(s"TESTS~LOU~${appConfs.TIME_PERIOD}") && row.cells.find(cell => {cell.column=="p_ENT" && cell.value.endsWith("TESTS")}).isDefined){
                    row.copy(key = s"$newLouLurn~LOU~${appConfs.TIME_PERIOD}", cells = row.cells.map(cell => if(cell.column=="p_ENT" && cell.value.endsWith("TESTS")){
                      cell.copy(value=newEntErn)
                    }else cell
                    ).toList.sortBy(_.column))


                 } else {
                  row.copy(cells = row.cells.toList.sortBy(_.column))
                }
          })

  val withNewMissingLouUpdated = withNewEntRowUpdated.map(row =>
    if(row.key.endsWith(s"TESTS~LOU~${appConfs.TIME_PERIOD}")&& row.cells.find(cell => cell.column=="p_ENT" && cell.value==entWithMissingLouId).isDefined){
      row.copy(key = s"$missingLouLurn~LOU~${appConfs.TIME_PERIOD}", cells = row.cells.toList.sortBy(_.column))
    }else if(row.key==s"$entWithMissingLouId~ENT~${appConfs.TIME_PERIOD}"){
      row.copy(cells = row.cells.map(cell => if(cell.value=="LOU" && cell.column.endsWith("TESTS")){
                                              cell.copy(column = s"c_$missingLouLurn")
                                            }else cell).toList.sortBy(_.column))
     }else row.copy(cells = row.cells.toList.sortBy(_.column)) )

  withNewMissingLouUpdated
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
    saveToHFile(existingLinksForMissingLousScenario,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
    saveToHFile(louForLouMissingScenario,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
}


}

