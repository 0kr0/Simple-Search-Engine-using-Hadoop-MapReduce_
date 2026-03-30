#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input path is :"
echo $1


hdfs dfs -ls /
