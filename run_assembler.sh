#!/usr/bin/env bash

SPARK=~/spark/bin/spark-submit
JAR=~/ONS/sbr-enterprise-assembler/target/scala-2.11/sbr-enterprise-assembler-assembly-1.1.jar
DATA_DIR=~/ONS/sbr-enterprise-assembler/src/test/resources/data

$SPARK --master local[*] --verbose --class assembler.AssemblerMain $JAR -quorum localhost -port 2181  -environment local -seq 'localhost:2181' -geo $DATA_DIR/geo/test-dataset.csv -geoShort $DATA_DIR/geo/test_short-dataset.csv -bi $DATA_DIR/newperiod/newPeriod.json -parquet ~/bi -vat $DATA_DIR/newperiod/newPeriodVat.csv -paye $DATA_DIR/newperiod/newPeriodPaye.csv -create-parquet
