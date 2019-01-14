#!/usr/bin/env bash

# The default number of partitions is 200 and the recommended number of partitions is 2-3* number of cores.
# The default therefore assumes >= 100 cores, clearly not applicable if running locally

PARTITIONS=24
CORES=12
SPARK=~/spark/bin/spark-submit
JAR=~/ONS/sbr-enterprise-assembler/target/scala-2.11/sbr-enterprise-assembler-assembly-1.1.jar

export LD_LIBRARY_PATH=~/hadoop/lib/native/

time ${SPARK} --master local[${CORES}]  \
    --conf spark.sql.shuffle.partitions=${PARTITIONS} \
    --conf spark.debug.maxToStringFields=40 \
    --class AssemblerMain \
    --files application.conf \
    --conf spark.driver.extraJavaOptions=-Dconfig.file=application.conf \
    ${JAR}
