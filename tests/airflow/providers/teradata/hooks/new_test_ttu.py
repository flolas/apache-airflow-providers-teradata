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

import io
import os
import unittest
from unittest.mock import call, patch

from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.teradata.hooks.ttu import TtuHook
from airflow.utils import db


class TestTtuHook(unittest.TestCase):

    _simple_bteq = """SELECT CURRENT_DATE;
                      .IF ERRORCODE <> 0 THEN .QUIT 0300;.QUIT 0;"""

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='ttu_default',
                conn_type='ttu',
                host='localhost',
                login='login'
                password='password',
            )
        )

    def test_build_bteq_file(self):
        # Given
        hook = TtuHook(ttu_conn_id='ttu_default')
        conn = hook.get_conn()
        # When
        bteq = hook._prepare_bteq_script(self._simple_bteq,
                                        conn['host'],
                                        conn['login'],
                                        conn['password'],
                                        conn['bteq_output_width'],
                                        conn['bteq_session_encoding'],
                                        conn['bteq_quit_zero']
                                        )

        # Then
        expected_bteq = """
        .LOGON localhost/login,
        .SET WIDTH 65531;
        .SET SESSION CHARSET 'ASCII';
        SELECT CURRENT_DATE;
        .IF ERRORCODE <> 0 THEN .QUIT 0300;
        .QUIT 0;
        """
        self.assertEqual(expected_bteq, expected_bteq)
