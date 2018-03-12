package dao.parquet

import java.net.URI
import java.util

import global.Configs
import model.RowObject
import org.apache.crunch.io.hbase.HFileInputFormat
import org.apache.hadoop.hbase.{KeyValue, KeyValueUtil}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import test.model.Ent
//import org.apache.crunch.io.hbase.HFileInputFormat
/**
  *
  */
class ParquetDaoSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with TestData {

  import global.Configs._


  val jsonFilePath = "src/test/resources/data/smallWithNullValues.json"
  val linkHfilePath = "src/test/resources/data/links"
  val entHfilePath = "src/test/resources/data/enterprise"
  val parquetHfilePath = "src/test/resources/data/sample.parquet"

  override def beforeAll() = {


    updateConf(Array[String](
      "LINKS", "ons", linkHfilePath,
      "ENT", "ons", entHfilePath,
      parquetHfilePath,
      "localhost",
      "2181","201802"
    ))

    conf.set("hbase.mapreduce.inputtable", "ons:ENT")
    //conf.set("hbase.mapreduce.inputtable", "ons:LINKS")

    //global.Configs.conf.set("hbase.mapreduce.inputtable","ons:ENT")

  }

/*  override def afterAll() = {
    val parquet = File(parquetHfilePath)
    parquet.deleteRecursively()
    val linkHfile = File(linkHfilePath)
    linkHfile.deleteRecursively()
    val entHfile = File(entHfilePath)
    entHfile.deleteRecursively()
  }*/


  "assembler" should {
    "create hfiles populated with expected links data" in {

      val spark: SparkSession = SparkSession.builder().master("local[2]").appName("enterprise assembler").getOrCreate()
/*      ParquetDAO.jsonToParquet(jsonFilePath)(spark)
      ParquetDAO.parquetToHFile(spark)*/


      conf.set("hbase.mapreduce.inputtable", "ons:ENT")
      val kvBytes: RDD[KeyValue] = spark.sparkContext.newAPIHadoopFile(entHfilePath,classOf[HFileInputFormat], classOf[NullWritable], classOf[KeyValue], conf).map(v => v._2)

       val keyValuesRdd = kvBytes.map(kv => (
         Bytes.toString(kv.getRowArray).slice(kv.getRowOffset,kv.getRowOffset+kv.getRowLength),
         (Bytes.toString(kv.getQualifierArray).slice(kv.getQualifierOffset,kv.getQualifierOffset+kv.getQualifierLength),Bytes.toString(kv.getValueArray).slice(kv.getValueOffset,kv.getValueOffset+kv.getValueLength))
       )).groupByKey().map(Ent(_))

      val res: Array[Ent] = keyValuesRdd.collect.sortBy(_.ern)
      val expected = testEnterprises(res).sortBy(_.ern)
      res shouldBe expected


      spark.close()

    }



  }




}
