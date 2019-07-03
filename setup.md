# Setup Instructions
This project builds on AWS EC2 Ubuntu 18.04 instances, which has python3 pre-installed. 

## Install Java
```bash
sudo apt update
sudo apt install openjdk-8-jdk
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
```
Download Java dependencies:
```bash
wget http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
wget http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.1/hadoop-aws-2.7.1.jar
```
Under /usr/local/spark/conf/spark-defaults.conf:
- spark.executor.extraClassPath path-to-jar-file
- spark.driver.extraClassPath path-to-jar-file

## Install Scala
```bash
sudo apt install scala
```

## Setup passwordless SSH between master and worker nodes of Spark cluster
On master node:
```bash
sudo apt install openssh-server openssh-client
cd ./.ssh/
ssh-keygen -t rsa -P ""
```
enter: id_rsa
copy id_rsa.pub to all worker nodes under ~/.ssh/authorized_keys

## Install Spark
```bash
wget http://apache.claz.org/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
tar xvf spark-2.4.3-bin-hadoop2.7.tgz
sudo mv spark-2.4.3-bin-hadoop2.7/ /usr/local/spark
export PATH=/usr/local/spark/bin:$PATH
```
Test if working fine:
```sh /usr/local/spark/sbin/start-all.sh```
To stop: 
```sbin/stop-all.sh```

## Install PostgreSQL
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo service postgresql
sudo -u postgres -i
psql
```

## Install Airflow
```bash
pip3 install apache-airflow
pip3 install apache-airflow[hive]

export AIRFLOW_HOME=~/airflow
export PATH=$PATH:~/.local/bin

airflow initdb
airflow webserver
```


