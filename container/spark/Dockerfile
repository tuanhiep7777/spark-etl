FROM --platform=linux/amd64 public.ecr.aws/bitnami/spark:4.1.1

USER root
# set current work dir
WORKDIR /opt/bitnami/spark/jars

RUN apt-get update && apt-get install -y telnet

ENV VAULT_ADDR='http://vault:8200'
ENV VAULT_TOKEN="root"