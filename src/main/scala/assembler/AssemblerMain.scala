package assembler


import global.{AppParams, Configs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager
import spark.extensions.rdd.HBaseDataReader


object AssemblerMain extends HBaseDataReader{

  private def unsetScanner(config:Configuration) = config.unset(TableInputFormat.SCAN)

  private def setScanner(config:Configuration,regex:String, appParams:AppParams) = {

    val comparator = new RegexStringComparator(regex)
    val filter = new RowFilter(CompareOp.EQUAL, comparator)

    def convertScanToString(scan: Scan): String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      return Base64.encodeBytes(proto.toByteArray())
    }

    val scan = new Scan()
    scan.setFilter(filter)
    val scanStr = convertScanToString(scan)

    config.set(TableInputFormat.SCAN,scanStr)
  }

  def main(args: Array[String]) {
    Configs.conf.set("hbase.zookeeper.quorum", args(9))
    Configs.conf.set("hbase.zookeeper.property.clientPort", args(10))
    val appParams = AppParams(args.take(9)++args.takeRight(2))

    implicit val spark: SparkSession = SparkSession.builder()/*.master("local[*]")*/.appName("enterprise assembler").getOrCreate()

      val tableName = s"${appParams.HBASE_LINKS_TABLE_NAMESPACE}:${appParams.HBASE_LINKS_TABLE_NAME}"
      Configs.conf.set(TableInputFormat.INPUT_TABLE, tableName)
      val regex = ".*(?<!~ENT~"+{appParams.TIME_PERIOD}+")$"
      setScanner(Configs.conf,regex,appParams)
      val resRdd = readKvsFromHBase(Configs.conf)
      unsetScanner(Configs.conf)
      resRdd.take(5).map(_.toString).foreach(row => print(
      "="*10+
        row+'\n'+
        "="*10
    ))
   spark.stop()


    //printDeleteData(AppParams(appParams))
    //loadRefresh(AppParams(appParams))

     //loadFromParquet(AppParams(appParams))
    //loadFromJson(AppParams(appParams))
    //loadFromHFile(AppParams(appParams))

  }

}
