
@echo off

:: The default number of partitions is 200 and the recommended number of partitions is 2-3* number of cores.
:: The default therefore assumes >= 100 cores, clearly not applicable if running locally

SET PARTITIONS=24
SET CORES=12
SET SPARK=\users\paul\spark\bin\spark-submit
SET JAR=~\users\paul\ONS\sbr-enterprise-assembler\target\scala-2.11\sbr-enterprise-assembler-assembly-1.1.jar

%SPARK% --master local[%CORES%] ^
   --conf spark.sql.shuffle.partitions=%PARTITIONS% ^
   --class AssemblerMain --files application.conf ^
   --conf spark.debug.maxToStringFields=40 ^
   --conf spark.driver.extraJavaOptions=-Dconfig.file=application.conf ^
   %JAR%