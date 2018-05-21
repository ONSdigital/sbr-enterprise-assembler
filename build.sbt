name := "sbr-enterprise_assembler"

version := "1.0"

scalaVersion := "2.11.8"

lazy val Versions = new {
  val hbase = "1.2.6"
  val spark = "2.2.0"
}

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.4.2",
  "com.typesafe" % "config" % "1.3.2",
  ("org.apache.hbase" % "hbase-server" % Versions.hbase)
                                                      .exclude("com.sun.jersey","jersey-server")
                                                      .exclude("org.mortbay.jetty","jsp-api-2.1"),
  "org.apache.hbase" % "hbase-common" % Versions.hbase,
  "org.apache.hbase" %  "hbase-client" % Versions.hbase,
  ("org.apache.spark" %% "spark-core" % Versions.spark)
                                                      .exclude("aopalliance","aopalliance")
                                                      .exclude("commons-beanutils","commons-beanutils"),
  "org.apache.spark" %% "spark-sql" % Versions.spark,
  ("org.apache.crunch" % "crunch-hbase" % "0.15.0")   .exclude("com.sun.jersey","jersey-server")

)


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*)    => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.first
  case PathList("jersey",".", xs @ _*) => MergeStrategy.first
  case PathList("aopalliance","aopalliance", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*)         => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

mainClass in (Compile,run) := Some("assembler.AssemblerMain")

lazy val myParameters = Array("LINKS", "ons","l",
  "src/main/resources/data/links/hfile", "ENT","ons", "d",
   "src/main/resources/data/enterprise/hfile",
  "src/main/resources/data/sample.parquet",
  "localhost", "2181", "201802","src/main/resources/data/smallPaye.csv",
  "src/main/resources/data/smallVat.csv",
  "local","create")

lazy val initialPopulationParams = Array("LINKS", "ons","l",
  "src/main/resources/data/original/links/hfile", "ENT","ons", "d",
   "src/main/resources/data/original/enterprise/hfile",
  "src/main/resources/data/original/sample.parquet",
  "localhost", "2181", "201802","src/main/resources/data/original/originalPaye.csv",
  "src/main/resources/data/original/originalVat.csv",
  "local","create")


lazy val recsParams = Array("LINKS", "ons","l",
  "src/main/resources/data/temp/3recs/links/hfile", "ENT","ons", "d",
   "src/main/resources/data/temp/3recs/enterprise/hfile",
  "src/main/resources/data/temp/3recs/sample.parquet",
  "localhost", "2181", "201802","src/main/resources/data/smallPaye.csv",
  "src/main/resources/data/smallPaye.csv",
  "local","create")

lazy val recsParamsRefresh = Array("LINKS", "ons","l",
  "src/main/resources/data/temp/3recsRefresh/links/hfile", "ENT","ons", "d",
   "src/main/resources/data/temp/3recsRefresh/enterprise/hfile",
   "src/main/resources/data/temp/3recsRefresh/local-unit/hfile",
  "src/main/resources/data/temp/3recsRefresh/sample.parquet",
  "localhost", "2181", "201802","src/main/resources/data/smallPaye.csv",
  "src/main/resources/data/smallPaye.csv",
  "local","refresh")





lazy val createRecordsParams = Array(
                                    "LINKS", "ons","l","src/main/resources/data/temp/3recsRefresh/links/hfile",
                                    "ENT","ons", "d", "src/main/resources/data/temp/3recsRefresh/enterprise/hfile",
                                    "LOU","ons","d","src/main/resources/data/temp/3recsRefresh/local-unit/hfile",
                                    "src/main/resources/data/temp/3recsRefresh/sample.parquet",
                                    "localhost", "2181", "201802","src/main/resources/data/smallPaye.csv",
                                    "src/main/resources/data/smallPaye.csv",
                                    "local","create"
                                  )


lazy val refreshRecordsParams = Array("unit_links", "sbr_dev_db","l",
"src/main/resources/data/temp/refresh/links/hfile", "enterprise","sbr_dev_db", "d",
"src/main/resources/data/temp/refresh/enterprise/hfile",
"src/main/resources/data/temp/refresh/sample.parquet",
"localhost", "2181", "201802","src/main/resources/data/smallPaye.csv",
"src/main/resources/data/smallPaye.csv",
"local","refresh")


lazy val addNewPeriodParams = Array("LINKS",
                                    "ons",
                                    "l",
                                    "src/main/resources/data/temp/addperiod/links/hfile",
                                    "ENT",
                                    "ons",
                                    "d",
                                    "src/main/resources/data/temp/addperiod/enterprise/hfile",
                                    "src/main/resources/data/temp/addperiod/sample.parquet",
                                    "localhost",
                                    "2181",
                                    "201804",
                                    "src/main/resources/data/newperiod/newPeriodPaye.csv",
                                    "src/main/resources/data/newperiod/newPeriodVat.csv",
                                    "local","addperiod")



lazy val deletePeriodParams = Array("LINKS",
                                    "ons",
                                    "l",
                                    "src/main/resources/data/temp/deleteperiod/links/hfile",
                                    "ENT",
                                    "ons",
                                    "d",
                                    "src/main/resources/data/temp/deleteperiod/enterprise/hfile",
                                    "src/main/resources/data/temp/deleteperiod/sample.parquet",
                                    "localhost",
                                    "2181",
                                    "201804",
                                    "src/main/resources/data/newperiod/newPeriodPaye.csv",
                                    "local","deleteperiod")


lazy val calculationsParams = Array("LINKS",
                                    "ons",
                                    "l",
                                    "src/main/resources/data/temp/addperiod/links/hfile",
                                    "ENT",
                                    "ons",
                                    "d",
                                    "src/main/resources/data/temp/addperiod/enterprise/hfile",
                                    "src/main/resources/data/temp/addperiod/sample.parquet",
                                    "localhost",
                                    "2181",
                                    "201804",
                                    "src/main/resources/data/smallPaye.csv",
                                    "src/main/resources/data/smallVat.csv",
                                    "local")



lazy val runWithArgs = taskKey[Unit]("run-args")
lazy val runRecs = taskKey[Unit]("run-args")
lazy val runRecsRefresh = taskKey[Unit]("run-args")
lazy val runCreateRecs = taskKey[Unit]("run-args")
lazy val runRefreshRecs = taskKey[Unit]("run-args")
lazy val runInitialPopulationRecs = taskKey[Unit]("run-args")
lazy val runAddPeriodRecs = taskKey[Unit]("run-args")
lazy val runCalculationPeriodRecs = taskKey[Unit]("run-args")
lazy val runDeletePeriod = taskKey[Unit]("run-args")


fullRunTask(runWithArgs, Runtime, "assembler.AssemblerMain", myParameters: _*)
fullRunTask(runRecs, Runtime, "assembler.AssemblerMain", recsParams: _*)
fullRunTask(runRecsRefresh, Runtime, "assembler.AssemblerMain", recsParamsRefresh: _*)
fullRunTask(runCreateRecs, Runtime, "assembler.AssemblerMain", createRecordsParams: _*)
fullRunTask(runRefreshRecs, Runtime, "assembler.AssemblerMain", refreshRecordsParams: _*)
fullRunTask(runAddPeriodRecs, Runtime, "assembler.AssemblerMain", addNewPeriodParams: _*)
fullRunTask(runInitialPopulationRecs, Runtime, "assembler.AssemblerMain", initialPopulationParams: _*)
fullRunTask(runCalculationPeriodRecs, Runtime, "assembler.AssemblerMain", calculationsParams: _*)
fullRunTask(runDeletePeriod, Runtime, "assembler.AssemblerMain", deletePeriodParams: _*)
