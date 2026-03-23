import json
from pyspark.sql import SparkSession
from common.constant import VAULT_ADDR, VAULT_TOKEN, GMAIL_USER, GMAIL_PASSWORD, SEND_EMAIL, SMTP_HOST, SMTP_PORT
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, BatchKwargs

def create_spark_session(kv_dict):
    # Load the default spark configurations
    spark_config = get_secret_from_vault("secret/data/spark/config")
    spark_builder = SparkSession.builder
    for key, value in spark_config.items():
        spark_builder = spark_builder.config(key,value)
    # Load additional configuration via dictionary
    for k, v in kv_dict.items():
        spark_builder = spark_builder.config(k,v)
    spark = spark_builder.getOrCreate()
    return spark

def get_secret_from_vault(secret_path):

    url = f"{VAULT_ADDR}/v1/{secret_path}"
    headers = {'X-Vault-Token': VAULT_TOKEN}

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()['data']['data']  # Adjust based on KV version
    else:
        response.raise_for_status()

def get_source_properties(jdbc_type) -> dict:
    # get from vault
    jdbc_dict = get_secret_from_vault(f'secret/data/{jdbc_type}')
    # check jdbc type
    if 'mysql' in jdbc_type:
        jdbc_dict['jdbc_driver'] = 'com.mysql.cj.jdbc.Driver' 
    elif 'oracle' in jdbc_type:
        jdbc_dict['jdbc_driver'] = 'oracle.jdbc.OracleDriver' 
    elif 'mssql' in jdbc_type:
        jdbc_dict['jdbc_driver'] = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'             
    return jdbc_dict

        
def _run_great_expectation(file_name, spark_df):
    # Convert the Spark DataFrame to a Great Expectations DataFrame
    ge_spark_df = ge.dataset.SparkDFDataset(spark_df)
    validation_results="Passed"
    try:
        # Load the JSON configuration
        with open(file_name, 'r') as file:
            config = json.load(file)
        
        # Loop through the JSON config and apply each expectation to the Spark DataFrame
        for expectation in config["expectations"]:
            expectation_type = expectation["expectation_type"]
            kwargs = expectation["kwargs"]

            # Dynamically call the corresponding Great Expectations method based on the expectation type
            ge_method = getattr(ge_spark_df, expectation_type)
            ge_method(**kwargs)

        validation_results = ge_spark_df.validate()
    except Exception as e:
        print("No Dq was defined")
    return validation_results
                
def _send_gmail(subject, body, to_email):
    # SMTP server configuration (supports both MailHog and Gmail)
    smtp_server = SMTP_HOST
    smtp_port = SMTP_PORT
    from_email = GMAIL_USER
    password = GMAIL_PASSWORD

    # Create the email
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject

    # Attach the email body
    msg.attach(MIMEText(body, "plain"))

    try:
        # Establish a connection to the server
        server = smtplib.SMTP(smtp_server, smtp_port)
        # Only use TLS and auth when a password is configured (e.g. Gmail)
        if password:
            server.starttls()
            server.login(from_email, password)

        # Send the email
        server.sendmail(from_email, to_email, msg.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        # Close the server connection
        server.quit()


def check_data_quality(file_name, df, table_name):
    validation_result=_run_great_expectation(file_name, df)
    # Check for validation failure
    if validation_result=="Passed":
        print("Validation passed.")  
    elif validation_result["success"]:
        print("Validation passed.")        
    else:
        print("Validation failed, sending email...")
        # Send email notification
        subject = f"Validation Failed: Great Expectations of table {table_name}" 
        body = f"Validation failed for column: numeric_column\nDetails: {validation_result}"
        _send_gmail(subject, body, SEND_EMAIL)
     
        
        
        
        
        
        
        
    