FROM apache/airflow:2.11.0-python3.11
RUN pip install apache-airflow-providers-microsoft-mssql
RUN pip install apache-airflow[odbc]
RUN pip install airflow-provider-tm1>=0.5.1
ADD plugins/tm1_bedrock_py-0.4.1-py3-none-any.whl .
RUN pip install tm1_bedrock_py-0.4.1-py3-none-any.whl[airflow,postgres]
#ADD airflow_provider_tm1 /tmp/airflow_provider_tm1
#ADD pyproject.toml /tmp/pyproject.toml
#ADD setup.py /tmp/setup.py
#ADD requirements.txt /tmp/requirements.txt
#WORKDIR /tmp

#USER root
#RUN pip install airflow_provider_tm1
#RUN chmod u+x setup.py airflow_provider_tm1/__init__.py && \
#    python -m pip install -r requirements.txt &&\
#    python -m build
#USER airflow
#RUN VERSION=$(grep '__version__ =' airflow_provider_tm1/__init__.py | awk -F '"' '{print $2}' | head -1) && \
#    pip install "dist/airflow_provider_tm1-$VERSION-py3-none-any.whl"