name := "sbr-enterprise-assembler"

version := "1.0"

scalaVersion := "2.11.8"

fork := true

parallelExecution in Test:= false

lazy val Versions = new {
  val hbase = "1.2.6"
  val spark = "2.2.0"
}

resolvers += "ClouderaRepo" at "https://repository.cloudera.com/artifactory/cloudera-repos"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.4.2",
  "com.typesafe" % "config" % "1.3.2",
  ("org.apache.hbase" % "hbase-server" % Versions.hbase)
    .exclude("com.sun.jersey","jersey-server")
    .exclude("org.mortbay.jetty","jsp-api-2.1"),
  "org.apache.hbase" % "hbase-common" % Versions.hbase,
  "org.apache.hbase" %  "hbase-client" % Versions.hbase,
  /*  ("org.apache.hbase" % "hbase-spark" % "2.0.0-alpha4")
      .exclude("com.fasterxml.jackson.module","jackson-module-scala_2.10"),*/
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


lazy val createRecordsParams = Array(
                                    "LINKS", "ons","l","src/main/resources/data/create/links/hfile",
                                    "ENT","ons", "d", "src/main/resources/data/create/enterprise/hfile",
                                    "LOU","ons","d","src/main/resources/data/create/local-unit/hfile",
                                    "src/main/resources/data/create/sample.parquet",
                                    "localhost",
                                    "2181",
                                    "201803",
                                    "src/main/resources/data/create/smallPaye.csv",
                                    "src/main/resources/data/create/smallVat.csv",
                                    "local",
                                    "create"
                                  )


lazy val refreshRecordsParams = Array("unit_links", "sbr_dev_db","l",
"src/main/resources/data/temp/refresh/links/hfile", "enterprise","sbr_dev_db", "d",
"src/main/resources/data/temp/refresh/enterprise/hfile",
"src/main/resources/data/temp/refresh/sample.parquet",
"localhost", "2181", "201802","src/main/resources/data/smallPaye.csv",
"src/main/resources/data/smallPaye.csv",
"local","refresh")


lazy val addNewPeriodParams = Array(
                                  "LINKS", "ons", "l", "src/main/resources/data/newperiod/links/hfile",
                                  "ENT", "ons", "d", "src/main/resources/data/newperiod/enterprise/hfile",
                                  "LOU", "ons", "d", "src/main/resources/data/newperiod/local-unit/hfile",
                                  "src/main/resources/data/newperiod/sample.parquet",
                                  "localhost",
                                  "2181",
                                  "201804",
                                  "src/main/resources/data/newperiod/newPeriodPaye.csv",
                                  "src/main/resources/data/newperiod/newPeriodVat.csv",
                                  "local",
                                  "addperiod"
                                  )


lazy val addNewPeriodWithCalculationsParams = Array(
                                  "LINKS", "ons", "l", "src/main/resources/data/newperiod/links/hfile",
                                  "LEU", "ons", "d", "src/main/resources/data/newperiod/legal-unit/hfile",
                                  "ENT", "ons", "d", "src/main/resources/data/newperiod/enterprise/hfile",
                                  "LOU", "ons", "d", "src/main/resources/data/newperiod/local-unit/hfile",
                                  "REU", "ons", "d", "src/main/resources/data/newperiod/reporting-unit/hfile",
                                  "src/main/resources/data/newperiod/sample.parquet",
                                  "localhost",
                                  "2181",
                                  "201804",
                                  "src/main/resources/data/newperiod/newPeriodPaye.csv",
                                  "src/main/resources/data/newperiod/newPeriodVat.csv",
                                  "local",
                                  "add-calculated-period"
                                )

//LINKS ons l src/main/resources/data/newperiod/links/hfile LEU ons d src/main/resources/data/newperiod/legal-unit/hfile ENT ons d src/main/resources/data/newperiod/enterprise/hfile LOU ons d src/main/resources/data/newperiod/local-unit/hfile src/main/resources/data/newperiod/sample.parquet localhost 2181 201804 src/main/resources/data/newperiod/newPeriodPaye.csv src/main/resources/data/newperiod/newPeriodVat.csv local add-calculated-period


lazy val deletePeriodParams = Array(
                                  "LINKS", "ons", "l", "src/main/resources/data/temp/deleteperiod/links/hfile",
                                  "ENT", "ons", "d", "src/main/resources/data/temp/deleteperiod/enterprise/hfile",
                                  "LOU", "ons", "d", "src/main/resources/data/temp/deleteperiod/local-unit/hfile",
                                  "",
                                  "localhost",
                                  "2181",
                                  "201804",
                                  "",
                                  "",
                                  "local",
                                  "deleteperiod"
                                )


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
                                    "local"
                                )



lazy val runWithArgs = taskKey[Unit]("run-args")
lazy val runRecs = taskKey[Unit]("run-args")
lazy val runRecsRefresh = taskKey[Unit]("run-args")
lazy val runCreateRecs = taskKey[Unit]("run-args")
lazy val runRefreshRecs = taskKey[Unit]("run-args")
lazy val runInitialPopulationRecs = taskKey[Unit]("run-args")
lazy val runAddPeriodRecs = taskKey[Unit]("run-args")
lazy val runCalculationPeriodRecs = taskKey[Unit]("run-args")
lazy val runDeletePeriod = taskKey[Unit]("run-args")
lazy val runAddPeriodWithCalculations = taskKey[Unit]("run-args")


fullRunTask(runCreateRecs, Runtime, "assembler.AssemblerMain", createRecordsParams: _*)
fullRunTask(runRefreshRecs, Runtime, "assembler.AssemblerMain", refreshRecordsParams: _*)
fullRunTask(runAddPeriodRecs, Runtime, "assembler.AssemblerMain", addNewPeriodParams: _*)
fullRunTask(runCalculationPeriodRecs, Runtime, "assembler.AssemblerMain", calculationsParams: _*)
fullRunTask(runDeletePeriod, Runtime, "assembler.AssemblerMain", deletePeriodParams: _*)
fullRunTask(runAddPeriodWithCalculations, Runtime, "assembler.AssemblerMain", addNewPeriodWithCalculationsParams: _*)

/*
current app args for addNewPeriod:
LINKS ons l src/main/resources/data/newperiod/links/hfile ENT ons d src/main/resources/data/newperiod/enterprise/hfile LOU ons d src/main/resources/data/newperiod/local-unit/hfile src/main/resources/data/newperiod/sample.parquet localhost 2181 201804 src/main/resources/data/newperiod/newPeriodPaye.csv src/main/resources/data/newperiod/newPeriodVat.csv local addperiod
*/

/*current app params for create:
* LINKS ons l src/main/resources/data/create/links/hfile ENT ons d src/main/resources/data/create/enterprise/hfile LOU ons d src/main/resources/data/create/local-unit/hfile src/main/resources/data/create/sample.parquet localhost 2181 201802 src/main/resources/data/create/smallPaye.csv main/resources/data/create/smallVat.csv local create
* */
