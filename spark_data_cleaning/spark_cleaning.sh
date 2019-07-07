#!/bin/bash
/usr/local/spark/bin/spark-submit --master spark://10.0.0.4:7077 --jars /usr/local/spark/lib/aws-java-sdk-1.7.4.jar,/usr/local/spark/lib/hadoop-aws-2.7.1.jar,/usr/local/spark/lib/postgresql-42.2.5.jar /home/ubuntu/project/spark_data_cleaning/clean_events.py
