# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging

from typing import Iterable, Mapping, Optional, Union


from airflow.providers.teradata.hooks.ttu import TtuHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class FastLoadOperator(BaseOperator):
    """
    Load a CSV file (without header) to Teradata Database Table
    :param input_file: output file to export
    :type input_file: str
    :param target_table: target table
    :type target_table: str
    :param delimiter: file delimiter
    :type delimiter: str
    :param working_database: database to create log tables
    :type working_database: str
    :param encoding: encoding of the file
    :type encoding: str
    :param xcom_push: True if return last log to xcom
    :type xcom_push: Bool
    """
    template_fields = ('input_file', 'target_table', 'preoperator_bteq',)
    template_ext = ('.sql', '.bteq',)
    ui_color = '#4aa3ba'

    @apply_defaults
    def __init__(
    self,
    *,
    input_file: str,
    target_table: str,
    working_database: str,
    delimiter: str = ';',
    encoding: str = 'UTF8',
    preoperator_bteq: Optional[str],
    raise_on_rows_error: bool = True,
    raise_on_rows_duplicated: bool = True,
    xcom_push: bool = True,
    ttu_conn_id: str = 'ttu_default',
    **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.input_file = input_file
        self.target_table = target_table
        self.working_database = working_database
        self.delimiter = delimiter
        self.encoding = encoding
        self.xcom_push = xcom_push
        self._hook = None
        self.ttu_conn_id = ttu_conn_id
        self.preoperator_bteq = preoperator_bteq
        self.raise_on_rows_error = raise_on_rows_error
        self.raise_on_rows_duplicated = raise_on_rows_duplicated

    def execute(self, context):
        """
        Call the executable from teradata
        """
        self._hook = TtuHook(ttu_conn_id=self.ttu_conn_id)

        if self.preoperator_bteq:
            logging.info('Executing preoperator BTEQ')
            self._hook.execute_bteq(self.preoperator_bteq, self.xcom_push)


        logging.info('Executing tdload command')
        self._hook.execute_tdload(
                            input_file=self.input_file,
                            table=self.target_table,
                            working_database=self.working_database,
                            delimiter=self.delimiter,
                            encoding=self.encoding,
                            xcom_push_flag=self.xcom_push,
                            raise_on_rows_error=self.raise_on_rows_error,
                            raise_on_rows_duplicated=self.raise_on_rows_duplicated)
    def on_kill(self):
        self._hook.on_kill()
