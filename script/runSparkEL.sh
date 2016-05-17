#!/bin/bash
#Inputs
numIterations=$1
ontology=$2
hadoopOntologyPath="hdfs://10.0.0.5:8020/user/azureuser/sparkel/ontologies/$ontology"
hadoopOutput="hdfs://10.0.0.5:8020/user/azureuser/sparkel/output/$ontology"
numNodes=$3
numPartitions=$4
parallelism=$5
logFolder=$6

#change to sparkel directory
cd "/home/azureuser/sparkel"

#print the headers
echo Run output for $ontology :
echo runID	nodes	partitions	runtime
#execute spark code
for (( c=1; c<=$numIterations; c++ ))
do
 logFile="${logFolder}_run${c}.txt"
 /home/azureuser/spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class "org.daselab.sparkel.SparkELHDFSTestCopy" --master spark://sparkel01:7077 --conf spark.driver.memory=12g --conf spark.executor.memory=12g --conf spark.local.dir=/mnt/sparktmp --conf spark.default.parallelism=$parallelism --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrationRequired=true --conf spark.shuffle.reduceLocality.enabled=false target/scala-2.10/sparkel_2.10-0.1.0.jar $hadoopOntologyPath $hadoopOutput $numNodes $numPartitions > $logFile
stats=$(tail -1 $logFile)
echo $c $stats
if [ $c -lt $numIterations ]
then
	sleep 10s
fi
done
