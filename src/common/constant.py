from dotenv import load_dotenv
import os

load_dotenv('/opt/airflow/.env')

VAULT_ADDR = os.getenv('VAULT_ADDR')
VAULT_TOKEN = os.getenv('VAULT_TOKEN')

SMTP_HOST = os.getenv('SMTP_HOST', 'mailhog')
SMTP_PORT = int(os.getenv('SMTP_PORT', '1025'))
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_PASSWORD = os.getenv('GMAIL_PASSWORD')
SEND_EMAIL = os.getenv('SEND_EMAIL')