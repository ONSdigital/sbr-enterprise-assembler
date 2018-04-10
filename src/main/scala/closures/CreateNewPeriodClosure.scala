package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.WithConversionHelper
import dao.parquet.ParquetDAO
import global.{AppParams, Configs}
import model.domain.{HFileRow, KVCell}
import model.hfile
import model.hfile.HFileCell
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


trait CreateNewPeriodClosure extends WithConversionHelper{


  type Cells = Iterable[KVCell[String, String]]
  type Record = (String, Cells)
  /**
    * copies data from Enterprise and Local Units tables into a new period data
    **/
  def createNewPeriodHfiles(confs: Configuration, appParams: AppParams)(implicit spark: SparkSession): Unit = {
    //do enterprise. Local units to follow
    saveEnterpriseHFiles(confs, appParams, ".*ENT~"+{appParams.PREVIOUS_TIME_PERIOD}+"$") //.*(~201802)$ //.*(?!~ENT~)201802$
    saveLinksHFiles(confs, appParams, ".*(~ENT~"+{appParams.PREVIOUS_TIME_PERIOD}+")$")
  }


  def readNewPeriodData(confs: Configuration, appParams: AppParams, regex: String)(implicit spark: SparkSession) = {
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .load(appParams.PATH_TO_LEU_TO_ENT_CSV)
  }

  def hfileRowToParquetRow(row:HFileRow) = {

    val schema = new StructType()
      .add(StructField("id", LongType,true))
      .add(StructField("BusinessName", StringType,true))
      .add(StructField("UPRN", LongType,true))
      .add(StructField("PostCode", StringType,true))
      .add(StructField("IndustryCode", StringType,true))
      .add(StructField("LegalStatus", StringType,true))
      .add(StructField("TradingStatus", StringType,true))
      .add(StructField("Turnover", StringType,true))
      .add(StructField("EmploymentBands", StringType,true))
      .add(StructField("VatRefs", ArrayType(StringType,true),true))
      .add(StructField("PayeRefs", ArrayType(StringType,true),true))
      .add(StructField("CompanyNo", StringType,true))


    new GenericRowWithSchema(row.cells.map(_.value).toArray, schema)

  }

  def getNewLUs(confs: Configuration, appconf: AppParams) = {

    val updatedConfs = appconf.copy(TIME_PERIOD=appconf.PREVIOUS_TIME_PERIOD) //set period to previous to make join possible
    val updatesRdd: RDD[Record] = ParquetDAO.readParquet(updatedConfs).groupByKey().map(v => (v._1,v._2.map(kv => KVCell[String,String](kv.key,kv.value))))//.cache() //get LINKS updates from input parquet

    val updatesLuRdd: RDD[Record] = updatesRdd.filter(row => row._1.endsWith(s"LEU~${appconf.PREVIOUS_TIME_PERIOD}")) //select LU rows from new data
    //next 3 lines: select LU rows from hbase
    val linksTableName = s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
    val luRegex = ".*LEU~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val existingLuRdd: RDD[Record] = HBaseDao.readTableWithKeyFilter(confs:Configuration,appconf:AppParams, linksTableName, luRegex).map(row => (row.key,row.cells))
    //select ENT rows from hbase
    val entRegex = ".*ENT~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val entTableName = s"${appconf.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appconf.HBASE_ENTERPRISE_TABLE_NAME}"
    val existingEntRdd: RDD[Record] = HBaseDao.readTableWithKeyFilter(confs:Configuration,appconf:AppParams, entTableName, entRegex).map(row => (row.key,row.cells))//toHfileCells

    val joined: RDD[(String, (Cells, Option[Cells]))] = updatesLuRdd.leftOuterJoin(existingLuRdd)//.cache()

    val updatedExistingLUs: RDD[HFileRow] = joined.collect { case (key, (newCells, Some(oldCells))) => HFileRow(key,{newCells ++ Seq(oldCells.find(_.column=="p_ENT").get) })} // existing LUs updated with new cells

    val newLUs: RDD[HFileRow] = joined.collect { case (key, (cells, None)) => HFileRow(key, cells) } //new LUs

    //joined.unpersist()

    val existingLUsHFileReady: RDD[HFileRow] = updatedExistingLUs.map(row => row.copy(row.key.replace(s"LEU~${appconf.PREVIOUS_TIME_PERIOD}",s"LEU~${appconf.TIME_PERIOD}"))) //complete LU rows
    val additionalCells = updatesRdd.map(rec => HFileRow(rec._1.replace(s"LEU~${appconf.PREVIOUS_TIME_PERIOD}",s"LEU~${appconf.TIME_PERIOD}"), rec._2))
                                                                        .filter(row => row.key.matches(".*(?<!(ENT|LUE))~"+appconf.TIME_PERIOD+"$"))  //other cells for existing LU

    //updatesRdd.unpersist()

    val linksForExistingLUs: RDD[HFileRow] = existingLUsHFileReady.union(additionalCells) //hfile ready links records for existing LUs

    val newEntTree: RDD[hfile.Tables] = newLUs.map(hrow => toEnterpriseRecords(hfileRowToParquetRow(hrow),appconf))//.cache()
    val newEnts: RDD[(String, hfile.HFileCell)] =  newEntTree.flatMap(t => t.enterprises) //break into cells
    val newLinks: RDD[(String, hfile.HFileCell)] =  newEntTree.flatMap(t => t.links) //break into cells
    //newEntTree.unpersist()
    //Add new records to existing records
    val hCellsOfExistingLUs: RDD[(String, hfile.HFileCell)] = linksForExistingLUs.flatMap(_.toHfileCells(appconf.HBASE_LINKS_COLUMN_FAMILY))
    val allLinks = hCellsOfExistingLUs.union(newLinks)

    val existingEntHFileCells: RDD[(String, HFileCell)] = existingEntRdd.flatMap(record => record._2.map(cell => (record._1,hfile.HFileCell(record._1,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY,cell.column, cell.value))))

    val allEnts = existingEntHFileCells.union(newEnts)

    //save to hfile:
    allLinks.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
       .saveAsNewAPIHadoopFile(appconf.PATH_TO_LINKS_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],confs)

    allEnts.sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
       .saveAsNewAPIHadoopFile(appconf.PATH_TO_ENTERPRISE_HFILE,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],confs)

  }



  private def saveEnterpriseHFiles(confs: Configuration, appParams: AppParams, regex: String) = {
    HBaseDao.readEnterprisesWithKeyFilter(confs, appParams, regex)
      .map(row => row.copy(row.key.replace(s"~${appParams.PREVIOUS_TIME_PERIOD}",s"~${appParams.TIME_PERIOD}")))
        .sortBy(row => s"${row.key}")
          .flatMap(_.toPutHFileEntries(appParams.HBASE_ENTERPRISE_COLUMN_FAMILY))
            .saveAsNewAPIHadoopFile(appParams.PATH_TO_ENTERPRISE_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], confs)
  }


  private def saveLinksHFiles(confs: Configuration, appParams: AppParams, regex: String) = {
    HBaseDao.readLinksWithKeyFilter(confs, appParams, regex)
      .map(row => row.copy(row.key.replace(s"~${appParams.PREVIOUS_TIME_PERIOD}",s"~${appParams.TIME_PERIOD}")))
        .sortBy(row => s"${row.key}")
          .flatMap(_.toPutHFileEntries(appParams.HBASE_LINKS_COLUMN_FAMILY))
           .saveAsNewAPIHadoopFile(appParams.PATH_TO_LINKS_HFILE, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], confs)
  }


}

