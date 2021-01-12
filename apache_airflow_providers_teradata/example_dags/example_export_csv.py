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
This is an example dag that export select queries from Teradata Database to CSV file (without header)
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.teradata.operators.fastexport import FastExportOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='example_export_csv_to_teradata',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    export_csv_to_teradata = FastExportOperator(
        task_id='export_csv_to_teradata',
        ttu_conn_id='ttu_default',
        sql_select_stmt='''SEL UserName, CreateTimeStamp, SpoolSpace FROM dbc.Users''',
        output_file='dummy_import.csv',
    )
