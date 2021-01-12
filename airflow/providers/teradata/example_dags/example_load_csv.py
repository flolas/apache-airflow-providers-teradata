#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example dag that loads a CSV file without header to Teradata Database
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.teradata.operators.fastload import FastLoadOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='example_load_csv_to_teradata',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    load_csv_to_teradata = FastLoadOperator(
        task_id='load_csv_to_teradata',
        ttu_conn_id='ttu_default',
        input_file='/files/dags/dummy_load.csv',
        ## Pre-operator BTEQ for creating the table before data load ocurrs
        ## You can use macros too!
        preoperator_bteq="""
                    DROP TABLE SQLJ.dummy_load_UV;
                    DROP TABLE SQLJ.dummy_load_Log;
                    DROP TABLE SQLJ.dummy_load_ET;
                    DROP TABLE SQLJ.dummy_load;
                    CREATE TABLE SQLJ.dummy_load (
                        id int,
                        first_name varchar(100),
                        mail varchar(320),
                        phone_number varchar(20)
                    )
                    PRIMARY INDEX (id);
                    .IF ERRORCODE <> 0 THEN .QUIT 0301;
                    .QUIT 0;
        """,
        target_table='SQLJ.dummy_load',
        ## Working db is for saving internal tables of fastload process (error tables)
        working_database='SQLJ',
        delimiter='|',
        encoding='UTF8'
    )
