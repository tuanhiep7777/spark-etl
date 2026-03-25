# Use the official Apache Superset image
FROM apache/superset:4.1.1

# Set environment variables, including a secure SECRET_KEY
ENV SUPERSET_SECRET_KEY='your_generated_secret_key_here'
ENV SUPERSET_HOME=/app/superset
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py
ENV PYTHONPATH="${PYTHONPATH}:${SUPERSET_HOME}"

# Copy the Superset config file into the container
COPY --chmod=644 superset_config.py /app/superset_config.py

USER root
RUN mkdir -p /app/superset && chown -R superset:superset /app/superset
USER superset

RUN USER_NAME=$(whoami) && echo "Current user is: $USER_NAME"

# upgrade db first (creates the SQLite database)
RUN superset db upgrade
# create admin user
RUN superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@admin.com \
  --password admin
# init superset
RUN superset init

USER root

RUN apt-get update && apt-get install -y jq

RUN pip install clickhouse-sqlalchemy sqlalchemy==1.4.47

USER $USER_NAME
# Copy the ClickHouse connection init_script into the container
COPY init_script.sh /app/init_script.sh
# copy change mode execution
RUN chmod +x /app/init_script.sh
# copy dashboard
COPY dashboard/  /app/dashboard/
# Expose the default port for Superset
EXPOSE 8088

# Command to run the Superset web server
CMD ["/app/init_script.sh"]

