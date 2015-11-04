#!/bin/env bash
#PBS -l nodes=3:ppn=12
module load plgrid/apps/spark
start-multinode-spark-cluster.sh
$SPARK_HOME/bin/spark-submit --master spark://`hostname`:7077 \
    --class SimplePageRank  $HOME/ar-pagerank/target/scala-2.10/sparkpagerank_2.10-1.0.jar 
stop-multinode-spark-cluster.sh
