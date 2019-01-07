#!/usr/bin/env bash

SPARK=~/spark/bin/spark-submit
JAR=~/ONS/sbr-enterprise-assembler/target/scala-2.11/sbr-enterprise-assembler-assembly-1.1.jar

$SPARK --master local[*] --class assembler.AssemblerMain --files application.conf --conf spark.driver.extraJavaOptions=-Dconfig.file=application.conf $JAR