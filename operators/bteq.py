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

class BteqOperator(BaseOperator):
    """
    Executes BTEQ code in a specific Teradata database

    :param sql: the bteq code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: str
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ('sql',)
    template_ext = ('.sql', '.bteq',)
    ui_color = '#ff976d'

    @apply_defaults
    def __init__(
        self,
        *,
        bteq: str,
        xcom_push: bool = True,
        ttu_conn_id: str = 'ttu_default',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.sql = bteq
        self._hook = None
        self.xcom_push = xcom_push
        self.ttu_conn_id = ttu_conn_id

    def execute(self, context):
        """
        Call execute_bteq method from TttuHook to run the provided BTEQ string
        """
        self._hook = TtuHook(ttu_conn_id=self.ttu_conn_id)
        self._hook.execute_bteq(self.sql, self.xcom_push)

    def on_kill(self):
        self._hook.on_kill()
