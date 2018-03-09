name := "sbr-enterprise_assembler"

version := "1.0"

scalaVersion := "2.11.8"

lazy val Versions = new {
  val clouderaHBase = "1.2.0-cdh5.10.1"
  val clouderaHadoop = "2.6.0-cdh5.10.1"
  val spark = "2.1.0"
}
lazy val Constants = new {
  //orgs
  val apacheHBase = "org.apache.hbase"
  val apacheHadoop = "org.apache.hadoop"
}

resolvers += "cloudera" at "https://repository.cloudera.com/cloudera/cloudera-repos/"
resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"
resolvers += "central" at "http://repo1.maven.org/maven2/"

libraryDependencies ++= Seq(

  Constants.apacheHBase   % "hbase-common"                      % Versions.clouderaHBase,
  Constants.apacheHBase   % "hbase"                             % Versions.clouderaHBase,
  Constants.apacheHBase   % "hbase-common"                      % Versions.clouderaHBase   classifier "tests",
  Constants.apacheHBase   % "hbase-client"                      % Versions.clouderaHBase   exclude ("org.slf4j", "slf4j-api"),
  Constants.apacheHBase   % "hbase-hadoop-compat"               % Versions.clouderaHBase,
  Constants.apacheHBase   % "hbase-hadoop-compat"               % Versions.clouderaHBase   classifier "tests",
  Constants.apacheHBase   % "hbase-hadoop2-compat"              % Versions.clouderaHBase,
  Constants.apacheHBase   % "hbase-hadoop2-compat"              % Versions.clouderaHBase   classifier "tests",
  Constants.apacheHBase   % "hbase-server"                      % Versions.clouderaHBase   classifier "tests",

  "org.apache.crunch"     % "crunch-hbase"                      % "0.11.0-cdh5.13.1",

  // Hadoop
  Constants.apacheHadoop  % "hadoop-common"                     % Versions.clouderaHadoop,
  Constants.apacheHadoop  % "hadoop-common"                     % Versions.clouderaHadoop  classifier "tests",
  Constants.apacheHadoop  % "hadoop-hdfs"                       % Versions.clouderaHadoop  exclude ("commons-daemon", "commons-daemon"),
  Constants.apacheHadoop  % "hadoop-hdfs"                       % Versions.clouderaHadoop  classifier "tests",
  Constants.apacheHadoop  % "hadoop-mapreduce-client-core"      % Versions.clouderaHadoop,
  Constants.apacheHadoop  % "hadoop-mapreduce-client-jobclient" % Versions.clouderaHadoop,
  "org.apache.hadoop" % "hadoop-minicluster" % "2.6.0-cdh5.10.1" % Test,


  "com.typesafe"          % "config"                            % "1.3.2",
  (Constants.apacheHBase  %  "hbase-server"                     % Versions.clouderaHBase).exclude("com.sun.jersey","jersey-server"),
  ("org.apache.spark"     %% "spark-core"                       % Versions.spark).exclude("aopalliance","aopalliance").exclude("commons-beanutils","commons-beanutils"),
  "org.apache.spark"      %% "spark-sql"                        % Versions.spark

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


mainClass in (Compile, packageBin) := Some("assembler.AssemblerMain")