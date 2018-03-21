package dao.parquet

import global.AppParams
import model.domain.{Enterprise, HFileCell, HFileRow}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.collection.immutable
import scala.reflect.io.File
/**
  *
  */
class ParquetDaoSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with TestData{

  import global.Configs._


  //val jsonFilePath = "src/test/resources/data/smallWithNullValues.json"
  val jsonFilePath = "src/test/resources/data/3recs.json"
  val linkHfilePath = "src/test/resources/data/links"
  val entHfilePath = "src/test/resources/data/enterprise"
  val parquetHfilePath = "src/test/resources/data/sample.parquet"
  val payeFilePath = "src/test/resources/data/smallPaye.csv"

  val appConfs = AppParams(
    (Array[String](
      "LINKS", "ons", "l", linkHfilePath,
      "ENT", "ons", "d",entHfilePath,
      parquetHfilePath,
      "localhost",
      "2181","201802",payeFilePath
    )))


  override def beforeAll() = {

    conf.set("enterprise.data.timeperiod", "default")

  }

  override def afterAll() = {
    File(parquetHfilePath).deleteRecursively()
    File(linkHfilePath).deleteRecursively()
    File(entHfilePath).deleteRecursively()
  }


  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
      ParquetDAO.jsonToParquet(jsonFilePath)(spark,appConfs)
      ParquetDAO.parquetToHFile(spark,appConfs)

      val actual: List[Enterprise] = readEntitiesFromHFile[Enterprise](entHfilePath).collect.toList.sortBy(_.ern)
      val expected: List[Enterprise] = testEnterprises3Recs(actual).sortBy(_.ern).toList
      actual shouldBe expected


      spark.close()

    }
  }



  "assembler" should {
    "create hfiles populated with expected links data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()


      ParquetDAO.jsonToParquet(jsonFilePath)
      ParquetDAO.parquetToHFile

       def replaceDynamicEntIdWithStatic(entLinks:Seq[HFileRow]) = {
         val erns = entLinks.collect{ case row if(row.key.contains("~ENT~")) => }
       }

      val actual: Seq[HFileRow] = readEntitiesFromHFile[HFileRow](linkHfilePath).collect.toList.sortBy(_.cells.map(_.column).mkString)
      val erns: Seq[(String, Int)] = actual.collect{case row if(row.cells.find(_.column=="p_ENT").isDefined) => {row.cells.collect{case HFileCell("p_ENT",value) => value}}}.flatten.zipWithIndex
      val ernsDictionary: Seq[(String, String)] = erns.map(ernTup => {
        val (ern,index) = ernTup
        (ern,{
          "testEnterpriseId-"+({index+1}.toString*5)
        })

      })

      //replace erns in actual:
      val actualUpdated = actual.map {
        case row  => {
                  if (ernsDictionary.find(_._1 == row.key.slice(0, 18)).isDefined) {
                    val ern = ernsDictionary.find(_._1 == row.key.slice(0, 18))
                    val rr: HFileRow = HFileRow(ern.get._2, row.cells)
                    rr

                  }else if(row.cells.find(cell => cell.column=="p_ENT").isDefined) {
                            row.copy(row.key, row.cells.map {cell =>
                              if (cell.column == "p_ENT"){
                                val value = ernsDictionary.find(_._1 == cell.value)
                                cell.copy(value = value.get._2)
                              }else cell
                            })
                  }else row
        }
      }
      val expected = testLinkRows3Recs
      actualUpdated shouldBe expected


      spark.close()
    }
  }




}
