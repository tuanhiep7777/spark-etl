FROM apache/airflow:3.0.6-python3.12
COPY --chmod=644 requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN USER_NAME=$(whoami) && echo "Current user is: $USER_NAME"

COPY --chmod=644 setup_conn.py $AIRFLOW_HOME

USER root
RUN mkdir -p /opt/jars && \
    USER_NAME=$(whoami) && \
    echo "Current user is: $USER_NAME" && \
    chown $USER_NAME /opt/jars

RUN apt-get update && \
    apt-get install -y --no-install-recommends \    
    default-jdk   
RUN apt-get install -y unzip --no-install-recommends
RUN apt-get install -y telnet --no-install-recommends

ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk-arm64'
ENV PATH=$PATH:$JAVA_HOME/bin    
ENV CLICKHOUSE_HOST=clickhouse
ENV CLICKHOUSE_USER=admin
ENV CLICKHOUSE_PASSWORD=admin
ENV MINIO_USER=minioadmin
ENV MINIO_PASSWORD=minioadmin
ENV SEND_EMAIL_TO=admin@local.test
# Set environment variables to configure airflow.cfg
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__SMTP__SMTP_HOST=mailhog
ENV AIRFLOW__SMTP__SMTP_USER=
ENV AIRFLOW__SMTP__SMTP_PASSWORD=
ENV AIRFLOW__SMTP__SMTP_PORT=1025
ENV AIRFLOW__SMTP__SMTP_STARTTLS=False
ENV AIRFLOW__SMTP__SMTP_MAIL_FROM=noreply@local.test


USER $USER_NAME

