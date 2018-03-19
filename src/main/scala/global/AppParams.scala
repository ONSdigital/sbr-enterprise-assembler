package global

/**
  * args sample:  LINKS ons l src/main/resources/data/links/hfile ENT ons d src/main/resources/data/enterprise/hfile src/main/resources/data/sample.parquet localhost 2181 201802 src/main/resources/data/smallPaye.csv
  * args types:   linksTableName linksNameSpace linksTablefamily linksHfilePath enterpriseTableName enterpriseNameSpace enterpriseTableFamily enterpriseHFilePath parquetFilePath quorumHost quorumPort timePeriod payeCsvPath
  */
case class AppParams(
                        HBASE_LINKS_TABLE_NAME:String,
                        HBASE_LINKS_TABLE_NAMESPACE:String,
                        HBASE_LINKS_COLUMN_FAMILY:String,
                        PATH_TO_LINKS_HFILE:String,
                        HBASE_ENTERPRISE_TABLE_NAME:String,
                        HBASE_ENTERPRISE_TABLE_NAMESPACE:String,
                        HBASE_ENTERPRISE_COLUMN_FAMILY:String,
                        PATH_TO_ENTERPRISE_HFILE:String,
                        PATH_TO_PARQUET:String,
                        QUORUM_HOST:String,
                        QUORUM_PORT:String,
                        TIME_PERIOD:String,
                        PATH_TO_PAYE:String
                   )

object AppParams{

  def apply(args:Array[String]) = new AppParams(
                                                  args(0),
                                                  args(1),
                                                  args(2),
                                                  args(3),
                                                  args(4),
                                                  args(5),
                                                  args(6),
                                                  args(7),
                                                  args(8),
                                                  args(9),
                                                  args(10),
                                                  args(11),
                                                  args(12)
                                                )
  
}
