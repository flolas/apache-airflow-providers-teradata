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
This is an example dag that runs a BTEQ sentence to teradata
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.teradata.operators.bteq import BteqOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='example_execute_bteq',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    task_execute_bteq = BteqOperator(
        task_id='task_execute_bteq',
        bteq='''
                 SELECT CURRENT_DATE;
                .IF ERRORCODE <> 0 THEN .QUIT 0301;
                .QUIT 0;
             ''',

    )
