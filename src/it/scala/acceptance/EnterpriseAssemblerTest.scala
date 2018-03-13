package acceptance

import java.util
import global.Configs
import global.Configs.{HBASE_ENTERPRISE_TABLE_NAME, conf}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest._
import service.EnterpriseAssemblerService
import model.domain.Enterprise
/**
  *
  */
class EnterpriseAssemblerTest extends WordSpecLike with Matchers with BeforeAndAfterAll with TestData{

  override def beforeAll() = {
                                global.Configs.updateConf(Array[String](
                                  "LINKS", "ons", "src/test/resources/data/links/hfile",
                                  "ENT", "ons", "src/test/resources/data/enterprise/hfile",
                                  "src/test/resources/data/sample.parquet",
                                  "localhost",
                                  "2181"
                                ))
    global.Configs.conf.set("hbase.mapreduce.inputtable","ons:ENT")
  }
 /**
   * assuming HBase is up and running
   * */
 "assembler" should {
     "create and populate hbase tables 'ENT' and 'LINKS' with expected data" in{

       val assembler = new EnterpriseAssemblerService{}
       assembler.loadFromJson

       val connection: Connection = ConnectionFactory.createConnection(Configs.conf)
       val tn: TableName = TableName.valueOf(conf.getStrings("hbase.table.enterprise.namespace").head, HBASE_ENTERPRISE_TABLE_NAME)//HBASE_ENTERPRISE_TABLE_NAME.map(ns => TableName.valueOf(ns, tableName)).getOrElse(TableName.valueOf(tableName))
       val table: Table = connection.getTable(tn)
       val admin = connection.getAdmin

       val job = Job.getInstance(connection.getConfiguration)
       job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
       job.setMapOutputValueClass(classOf[KeyValue])

       val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()

       val valuesBytes: RDD[(Result)] = spark.sparkContext.newAPIHadoopRDD(Configs.conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)


       val navMap: RDD[util.NavigableMap[Array[Byte], Array[Byte]]] = valuesBytes.map(_.getFamilyMap("d".getBytes()))

       val str = navMap.map(Enterprise(_))

       val res: Array[Enterprise] = str.collect.sortBy(_.ern)
       val expected = testEnterprises(res).sortBy(_.ern)
       res shouldBe expected


       admin.close
       table.close
       spark.close()

     }
 }


}
