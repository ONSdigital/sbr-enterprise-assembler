package assembler


import global.Configs.conf
import global.{AppParams, Configs}
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object AssemblerMain{

  def readWithKeyFilter(configs:Configuration,appParams:AppParams,regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {

    val tableName = s"${appParams.HBASE_LINKS_TABLE_NAMESPACE}:${appParams.HBASE_LINKS_TABLE_NAME}"
    configs.set(TableInputFormat.INPUT_TABLE, tableName)
    //val regex = "72~LEU~"+{appParams.TIME_PERIOD}+"$"
    setScanner(regex,appParams)
    val data = readKvsFromHBase(spark)
    unsetScanner
    data
  }

  def unsetScanner = conf.unset(TableInputFormat.SCAN)

  def setScanner(regex:String, appParams:AppParams) = {

    val comparator = new RegexStringComparator(regex)
    val filter = new RowFilter(CompareOp.EQUAL, comparator)

    def convertScanToString(scan: Scan): String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      return Base64.encodeBytes(proto.toByteArray())
    }

    val scan = new Scan()
    scan.setFilter(filter)
    val scanStr = convertScanToString(scan)

    conf.set(TableInputFormat.SCAN,scanStr)
  }

  def readKvsFromHBase(implicit spark:SparkSession): RDD[HFileRow] =
    spark.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map(_._2).map(HFileRow(_))

  def main(args: Array[String]) {
    Configs.conf.set("hbase.zookeeper.quorum", args(9))
    Configs.conf.set("hbase.zookeeper.property.clientPort", args(10))
    val appParams = args.take(9)++args.takeRight(2)
    val appconf  = AppParams(appParams)
    implicit val spark: SparkSession = SparkSession.builder()./*master("local[*]").*/appName("enterprise assembler").getOrCreate()
    val regex = ".*(?<!~ENT~"+{appconf.TIME_PERIOD}+")$"
    val rdd: RDD[HFileRow] = readWithKeyFilter(Configs.conf,appconf,regex)
    val rows: Array[HFileRow] = rdd.take(5)
    rows.map(_.toString).foreach(row => print(
      "="*10+
        row+'\n'+
        "="*10
    )

    )
    spark.stop()
  }

}
