####################################################################################################################
# Setup containers to run
up:
	docker compose up -d

zip-code:
	cd src/ && zip -r dependencies.zip common/ jobs/ 
copy-code:
	cd src/ && docker cp main.py scheduler:/opt/airflow
	cd src/ && docker cp dependencies.zip scheduler:/opt/airflow
	docker cp container/airflow/lib scheduler:/opt/jars
	docker cp .env scheduler:/opt/airflow
	cd src/ && docker cp config scheduler:/opt/airflow
	cd dbt/ && docker cp dbt_clickhouse scheduler:/opt/airflow
chmod:
	docker exec --user root scheduler chmod -R 755 /opt/jars/lib
	docker exec --user root scheduler chmod 755 /opt/airflow/main.py
	docker exec --user root scheduler chmod 755 /opt/airflow/dependencies.zip
	docker exec --user root scheduler chmod -R 755 /opt/airflow/config
	docker exec --user root scheduler chmod -R 755 /opt/airflow/dbt_clickhouse
	docker exec --user root scheduler mkdir -p /opt/airflow/dbt_clickhouse/logs /opt/airflow/dbt_clickhouse/target
	docker exec --user root scheduler chmod -R 777 /opt/airflow/dbt_clickhouse/logs
	docker exec --user root scheduler chmod -R 777 /opt/airflow/dbt_clickhouse/target

clean-zip:
	rm -rf src/dependencies.zip

sleep:
	sleep 15

all: up sleep zip-code copy-code chmod clean-zip

dashboard:
	docker exec -it superset superset import-directory -o /app/dashboard

down: 
	docker compose down -v
	docker images | grep spark-etl | awk '{print $3}' | xargs docker rmi -f
