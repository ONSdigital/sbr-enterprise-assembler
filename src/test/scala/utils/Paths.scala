package utils

/**
  *
  */
trait Paths {this: { val testDir: String } =>

  val jsonOrigFilePath = s"src/test/resources/data/$testDir/newPeriod-orig.json"
  val jsonFilePath = s"src/test/resources/data/$testDir/newPeriod.json"
  val linkHfilePath = s"src/test/resources/data/$testDir/links"
  val linkDeleteHfilePath = s"src/test/resources/data/$testDir/delete/links"
  val leuHfilePath = s"src/test/resources/data/$testDir/leu"
  val leuDeleteHfilePath = s"src/test/resources/data/$testDir/delete/leu"
  val entHfilePath = s"src/test/resources/data/$testDir/enterprise"
  val entDeleteHfilePath = s"src/test/resources/data/$testDir/delete/enterprise"
  val louHfilePath = s"src/test/resources/data/$testDir/lou"
  val ruHfilePath = s"src/test/resources/data/$testDir/rou"
  val louDeleteHfilePath = s"src/test/resources/data/$testDir/delete/lou"
  val parquetPath = s"src/test/resources/data/$testDir/sample.parquet"
  val payeFilePath = s"src/test/resources/data/$testDir/newPeriodPaye.csv"
  val vatFilePath = s"src/test/resources/data/$testDir/newPeriodVat.csv"
  val geoFilePath = "src/test/resources/data/geo/GEO_4_TESTS.csv"
  val existingRecordsDir = s"src/test/resources/data/$testDir/existing"
  val existingEntRecordHFiles = s"$existingRecordsDir/enterprise"
  val existingLinksRecordHFiles = s"$existingRecordsDir/links"
  val existingLeusRecordHFiles = s"$existingRecordsDir/leu"
  val existingLousRecordHFiles = s"$existingRecordsDir/lou"
  val existingRusRecordHFiles = s"$existingRecordsDir/rou"
}

