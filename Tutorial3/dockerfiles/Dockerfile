FROM puckel/docker-airflow:1.10.9

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV PYTHONPATH=$PYTHONPATH:${AIRFLOW_USER_HOME}

# Python packages required for th Selenium Plugin
USER root

RUN pip install docker && \
    pip install selenium && \
    pip install bs4 && \
    pip install pandas && \
    pip install pymongo && \
    pip install requests && \
    pip install swifter && \
    pip install numpy && \
    pip install lxml && \
    pip install flask-bcrypt && \
    pip install boto3


RUN groupadd --gid 999 docker \
   && usermod -aG docker airflow

CMD airflow create_user  --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

USER airflow

