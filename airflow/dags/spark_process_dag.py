# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

#initial dag object
dag = DAG(
    dag_id='spark_process_dag',
    description='data extract from public s3 -> store back to s3 in parquet -> compute and write data to postgres',
    default_args=default_args)

#step1a events data loading & cleaning
event_clean = BashOperator(
    task_id='event_clean',
    bash_command='/usr/local/spark/bin/spark-submit --master spark://10.0.0.4:7077 '+\
                '--jars /usr/local/spark/lib/aws-java-sdk-1.7.4.jar,/usr/local/spark/lib/hadoop-aws-2.7.1.jar,'+\
                '/usr/local/spark/lib/postgresql-42.2.5.jar /home/ubuntu/project/spark_data_cleaning/airflow_clean_events.py'+\
                ' {{ ds_nodash }}',
    dag = dag)

#step1b mentions data loading & cleaning
mentions_clean = BashOperator(
    task_id='mentions_clean',
    bash_command='/usr/local/spark/bin/spark-submit --master spark://10.0.0.4:7077 '+\
                '--jars /usr/local/spark/lib/aws-java-sdk-1.7.4.jar,/usr/local/spark/lib/hadoop-aws-2.7.1.jar,'+\
                '/usr/local/spark/lib/postgresql-42.2.5.jar /home/ubuntu/project/spark_data_cleaning/airflow_clean_mentions.py'+\
                ' {{ ds_nodash }}',
    dag = dag)

#step1a gkg data loading & cleaning
gkg_clean = BashOperator(
    task_id='gkg_clean',
    bash_command='/usr/local/spark/bin/spark-submit --master spark://10.0.0.4:7077 '+\
                '--jars /usr/local/spark/lib/aws-java-sdk-1.7.4.jar,/usr/local/spark/lib/hadoop-aws-2.7.1.jar,'+\
                '/usr/local/spark/lib/postgresql-42.2.5.jar /home/ubuntu/project/spark_data_cleaning/airflow_clean_gkg.py'+\
                ' {{ ds_nodash }}',
    dag = dag)

#step2 write data to rdbs
store_event  = BashOperator(
    task_id='store_event',
    bash_command='/usr/local/spark/bin/spark-submit --master spark://10.0.0.4:7077 '+\
                '--jars /usr/local/spark/lib/aws-java-sdk-1.7.4.jar,/usr/local/spark/lib/hadoop-aws-2.7.1.jar,'+\
                '/usr/local/spark/lib/postgresql-42.2.5.jar /home/ubuntu/project/spark_process/airflow_store_events.py'+\
                ' {{ ds_nodash }}',
    dag = dag)

#step3 compute safety score and write to rdbs
compute_score  = BashOperator(
    task_id='compute_score',
    bash_command='/usr/local/spark/bin/spark-submit --master spark://10.0.0.4:7077 '+\
                '--jars /usr/local/spark/lib/aws-java-sdk-1.7.4.jar,/usr/local/spark/lib/hadoop-aws-2.7.1.jar,'+\
                '/usr/local/spark/lib/postgresql-42.2.5.jar /home/ubuntu/project/spark_process/airflow_batch_process.py'+\
                ' {{ ds_nodash }}',
    dag = dag)


# setting dependencies
event_clean >> store_event
[event_clean, mentions_clean, gkg_clean] >> compute_score
