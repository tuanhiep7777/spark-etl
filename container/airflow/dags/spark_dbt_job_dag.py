from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

# Get the email sent to variable
email_to = Variable.get("email_to")
# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 29),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'clickhouse_spark_dag',
    default_args=default_args,
    description='A PySpark job DAG with combined dependencies',
    schedule='@daily',
    catchup=False,
)

# Define the Spark submit task
spark_task_mysql_customer = SparkSubmitOperator(
    application='/opt/airflow/main.py',  # Path to your main PySpark script
    name='spark_job_mysql_customer',
    conn_id='spark-default',  # Connection ID configured in Airflow
    jars='/opt/jars/lib/*',
    py_files='/opt/airflow/dependencies.zip', 
    application_args=["--source_type", "jdbc_mysql", "--source_name", "sap", "--table_name", "customer"],
    executor_cores=2,
    executor_memory='2g',
    num_executors=2,
    driver_memory='1g',
    dag=dag,
    task_id='spark_job_load_mysql_customer',
    verbose=True,
)

spark_task_mysql_transaction = SparkSubmitOperator(
    application='/opt/airflow/main.py',  # Path to your main PySpark script
    name='spark_job_mysql_transaction',
    conn_id='spark-default',  # Connection ID configured in Airflow
    jars='/opt/jars/lib/*',
    py_files='/opt/airflow/dependencies.zip', 
    application_args=["--source_type", "jdbc_mysql", "--source_name", "sap", "--table_name", "transaction"],
    executor_cores=2,
    executor_memory='2g',
    num_executors=2,
    driver_memory='1g',
    dag=dag,
    task_id='spark_job_load_mysql_transaction',
    verbose=True,
)

spark_task_mongo_customer = SparkSubmitOperator(
    application='/opt/airflow/main.py',  # Path to your main PySpark script
    name='spark_task_mongo_customer',
    conn_id='spark-default',  # Connection ID configured in Airflow
    jars='/opt/jars/lib/*',
    py_files='/opt/airflow/dependencies.zip', 
    application_args=["--source_type", "mongodb", "--source_name", "crm", "--table_name", "customer"],
    executor_cores=2,
    executor_memory='2g',
    num_executors=2,
    driver_memory='1g',
    dag=dag,
    task_id='spark_task_mongo_customer',
    verbose=True,
)

spark_task_mongo_transaction = SparkSubmitOperator(
    application='/opt/airflow/main.py',  # Path to your main PySpark script
    name='spark_task_mongo_transaction',
    conn_id='spark-default',  # Connection ID configured in Airflow
    jars='/opt/jars/lib/*',
    py_files='/opt/airflow/dependencies.zip', 
    application_args=["--source_type", "mongodb", "--source_name", "crm", "--table_name", "transaction"],
    executor_cores=2,
    executor_memory='2g',
    num_executors=2,
    driver_memory='1g',
    dag=dag,
    task_id='spark_task_mongo_transaction',
    verbose=True,
)

dbt_raw = BashOperator(
    task_id='dbt_run_raw',
    bash_command="cd /opt/airflow/dbt_clickhouse && dbt run --models models/raw/ --profiles-dir . --target raw",
    dag=dag,
)

dbt_datamart = BashOperator(
    task_id='dbt_run_datamart',
    bash_command="cd /opt/airflow/dbt_clickhouse && dbt run --models models/datamart/ --profiles-dir . --target datamart",
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_run_test',
    bash_command="cd /opt/airflow/dbt_clickhouse && dbt test --models models/datamart/ --profiles-dir . --target datamart",
    dag=dag,
)
# Task to send success email
send_success_email = EmailOperator(
    task_id='send_success_email',
    to=email_to,
    subject='Task Succeeded',
    html_content='The task completed successfully!',
    dag=dag,
    trigger_rule = TriggerRule.ALL_SUCCESS
)

# Task to send failure email
send_failure_email = EmailOperator(
    task_id='send_failure_email',
    to=email_to,
    subject='Task Failed',
    html_content='The task failed. Please check the logs for details.',
    dag=dag,
    trigger_rule = TriggerRule.ONE_FAILED
)
# The task must be added to the DAG
[spark_task_mysql_customer,spark_task_mysql_transaction,spark_task_mongo_customer,spark_task_mongo_transaction] >> dbt_raw >> dbt_datamart >> dbt_test >> [send_success_email, send_failure_email]

