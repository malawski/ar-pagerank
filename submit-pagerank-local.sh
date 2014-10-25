#!/bin/env bash
#PBS -l nodes=1:ppn=12
 
source $PLG_GROUPS_STORAGE/plgg-spark/set_env_spark-1.0.0.sh
$SPARK_HOME/bin/spark-submit --class SimplePageRank --master local[*] $HOME/ar-pagerank/target/scala-2.10/sparkpagerank_2.10-1.0.jar

