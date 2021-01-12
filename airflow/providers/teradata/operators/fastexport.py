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
from typing import Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.teradata.hooks.ttu import TtuHook
from airflow.utils.decorators import apply_defaults

class FastExportOperator(BaseOperator):
    """
    Export a table from Teradata to csv file
    :param sql_select_stmt: Select statament to export
    :type sql_select_stmt: str
    :param output_file: output file to export
    :type output_file: str
    """
    template_fields = ('sql','output_file',)
    template_ext = ('.sql',)
    ui_color = '#a8e4b1'

    def __init__(
        self,
        *,
        sql_select_stmt: str,
        output_file: str,
        delimiter: str = ';',
        encoding: str = 'UTF8',
        xcom_push: bool = True,
        ttu_conn_id: str = 'ttu_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql_select_stmt
        self.output_file = output_file
        self.delimiter = delimiter
        self.encoding = encoding
        self.xcom_push = xcom_push
        self._hook = None
        self.ttu_conn_id = ttu_conn_id

    def execute(self, context):
        """
        Call the function
        """
        self._hook = TtuHook(ttu_conn_id=self.ttu_conn_id)
        self._hook.execute_tptexport(sql=self.sql,
                            output_file=self.output_file,
                            delimiter=self.delimiter,
                            encoding=self.encoding,
                            xcom_push_flag=self.xcom_push)
    def on_kill(self):
        self._hook.on_kill()
