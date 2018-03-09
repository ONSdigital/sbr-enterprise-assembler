package dao.parquet

import java.net.URI
import java.util

import global.Configs
import global.Configs._
import model.Ent
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, Table}
import org.apache.hadoop.hbase.{HBaseTestingUtility, KeyValue, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.scalatest._
import org.apache.spark.rdd._
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File
import dao.hbase._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.io.hfile._
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseTestCase
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.Tag
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.hfile.HFile.Reader
import org.apache.hadoop.hbase.io.hfile.HFile.Writer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Writable

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
      /*ParquetDAO.jsonToParquet(jsonFilePath)(spark)*/
      /*ParquetDAO.parquetToHFile(spark)*/
      //val connection = getConnection
      //HBaseDao.loadLinksHFile(connection)

      //val ENT_TABLE_NAMESPACE = conf.getStrings("hbase.table.enterprise.namespace").head

      //val valuesBytes = spark.sparkContext.newAPIHadoopFile(entHfilePath,classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result], conf).map(_._2)
      //val valuesBytes = spark.sparkContext.newAPIHadoopFile(entHfilePath,classOf[TableRecordReader], classOf[ImmutableBytesWritable], classOf[Result], conf).map(_._2)
      //val testUtil = new HBaseTestingUtility()
      //val rootDir = testUtil.getDataTestDir("src/test/resources/data/enterprise").toString()
      val cc = new CacheConfig(conf)
      val path = new Path(entHfilePath, "d")
      val fs  = FileSystem.get(new URI(entHfilePath),conf)
      val reader = HFile.createReader(fs,path,cc,Configs.conf)
      val info: util.Map[Array[Byte], Array[Byte]] = reader.loadFileInfo()

      /*val navMap: RDD[util.NavigableMap[Array[Byte], Array[Byte]]] = valuesBytes.map(_.getFamilyMap("d".getBytes()))

      val str = navMap.map(Ent(_))

      val res: Array[Ent] = str.collect//.sortBy(_.ern)
      val expected = testEnterprises(res).sortBy(_.ern)
      res shouldBe expected*/

      //connection.close()
      import scala.collection.JavaConversions._
      val resMap = info.map(kv => (new ImmutableBytesWritable(kv._1), new KeyValue(kv._2)))
      spark.close()

    }


    def getConnection:Connection = {
      val ENT_TABLE_NAMESPACE = conf.getStrings("hbase.table.enterprise.namespace").head
      val LINKS_TABLE_NAMESPACE = conf.getStrings("hbase.table.links.namespace").head

      val utility = new HBaseTestingUtility(conf)
      utility.startMiniCluster()
      utility.getHBaseAdmin.createNamespace(NamespaceDescriptor.create("ons").build())
      utility.createTable(TableName.valueOf("ons",HBASE_ENTERPRISE_TABLE_NAME), "d")
      utility.createTable(TableName.valueOf("ons",HBASE_LINKS_TABLE_NAME), "l")
      val testDir = utility.getDataTestDirOnTestFS
      val defaultDir = utility.getDefaultRootDirPath
      val dirOnFs = utility.getDataTestDirOnTestFS
      utility.setFileSystemURI(PATH_TO_LINKS_HFILE)
      utility.getConnection()
  }
  }




}
