#!/bin/env bash
#PBS -l nodes=3:ppn=12
 
source $PLG_GROUPS_STORAGE/plgg-spark/set_env_spark-1.0.0.sh
$SPARK_HOME/sbin/start-multinode-spark-cluster.sh
$SPARK_HOME/bin/spark-submit --master spark://`hostname`:7077 \
    --class SimplePageRank  $HOME/ar-pagerank/target/scala-2.10/sparkpagerank_2.10-1.0.jar 
$SPARK_HOME/sbin/stop-multinode-spark-cluster.sh
