FROM apache/airflow:2.9.1

#ADD airflow_provider_tm1 /tmp/airflow_provider_tm1
#ADD pyproject.toml /tmp/pyproject.toml
#ADD setup.py /tmp/setup.py
#ADD requirements.txt /tmp/requirements.txt
#WORKDIR /tmp

#USER root
RUN pip install airflow_provider_tm1
#RUN chmod u+x setup.py airflow_provider_tm1/__init__.py && \
#    python -m pip install -r requirements.txt &&\
#    python -m build
#USER airflow
#RUN VERSION=$(grep '__version__ =' airflow_provider_tm1/__init__.py | awk -F '"' '{print $2}' | head -1) && \
#    pip install "dist/airflow_provider_tm1-$VERSION-py3-none-any.whl"