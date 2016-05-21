#!/bin/bash

sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer

wget https://dl.bintray.com/sbt/native-packages/sbt/0.13.9/sbt-0.13.9.tgz
tar -xvzf sbt-0.13.9.tgz
echo "export PATH=$PATH:/home/azureuser/sbt/bin" >> ~/.profile
source ~/.profile

wget http://apache.go-parts.com/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
tar -xvzf spark-1.6.1-bin-hadoop2.6.tgz

wget http://apache.mirrors.ionfish.org/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
tar -xvzf hadoop-2.7.2.tar.gz

sudo apt-get install git

git clone https://github.com/raghavam/sparkel.git

cd sparkel

sbt package 
