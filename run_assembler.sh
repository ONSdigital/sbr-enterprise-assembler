#!/usr/bin/env bash

SPARK=~/spark/bin/spark-submit
JAR=~/ONS/sbr-enterprise-assembler/target/scala-2.11/sbr-enterprise-assembler-assembly-1.1.jar

export LD_LIBRARY_PATH=~/hadoop/lib/native/

$SPARK --master local[*] --executor-memory 2000M --class assembler.AssemblerMain --files application.conf --conf spark.driver.extraJavaOptions=-Dconfig.file=application.conf $JAR
