FROM apache/airflow:2.10.5

USER root

RUN apt-get update && apt-get install -y

# RUN apt-get update && apt-get install -y \
#     wget \
#     unzip \
#     && wget --quiet https://releases.hashicorp.com/terraform/1.10.5/terraform_1.10.5_linux_amd64.zip \
#     && unzip terraform_1.10.5_linux_amd64.zip \
#     && mv terraform /usr/local/bin/ \
#     && rm terraform_1.10.5_linux_amd64.zip \
#     && chmod +x /usr/local/bin/terraform \
#     && rm -rf /var/lib/apt/lists/*

# # Ensure the directories exist and set correct permissions
# RUN mkdir -p /opt/airflow/keys /opt/airflow/terraform && \
#     chmod -R 755 /opt/airflow/keys /opt/airflow/terraform && \
#     chown -R airflow:0 /opt/airflow/keys /opt/airflow/terraform

USER airflow

RUN cd $HOME && \
    curl -sSL https://sdk.cloud.google.com | bash -s -- --disable-prompts && \
    exec -l $SHELL && \
    echo 'export PATH="$HOME/google-cloud-sdk/bin:$PATH"' >> ~/.bashrc && \
    source ~/.bashrc && \
    ls -l $(which gcloud) && \
    chmod +x $(which gcloud)

RUN pip install apache-airflow-providers-ssh apache-airflow-providers-google paramiko dlt

# Entry point for Airflow webserver
# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["webserver"]
