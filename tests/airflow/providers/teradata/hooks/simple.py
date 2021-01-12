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


import logging
import mock
from io import TextIOWrapper, BytesIO, StringIO, BufferedReader, BufferedWriter
from tempfile import gettempdir, NamedTemporaryFile, TemporaryDirectory

LOGGER = logging.getLogger(__name__)

from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.teradata.hooks.ttu import TtuHook
from airflow.utils import db

from tests.test_utils.mock_process import MockSubProcess

class TestTtuHook(unittest.TestCase):
    _simple_bteq = """SELECT CURRENT_DATE;\n.IF ERRORCODE <> 0 THEN .QUIT 0300;"""

    _bteq_failure_subprocess_output = ("BTEQ 15.10.01.00 Thu Sep 17 05:01:48 2020 PID: 11951",
        " ",
        "+---------+---------+---------+---------+---------+---------+---------+----",
        ".LOGON localhost/dbc,",
        "*** Failure 3701 Login error",
        "SELECT * FROM test",
        "*** Failure 3706 Syntax error: expected something between '(' and the string 'test'",
        "+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-",
        ".IF ERRORCODE <> 0 THEN .QUIT 0311;",
        "+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-",
        )

    _bteq_error_no_failure_subprocess_output = ("BTEQ 15.10.01.00 Thu Sep 17 05:01:48 2020 PID: 11951",
        " ",
        "+---------+---------+---------+---------+---------+---------+---------+----",
        ".LOGON localhost/dbc,",
        "*** Failure 3706 Syntax error: expected something between '(' and the string 'test'",
        "+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-",
        ".QUIT 0;",
        "+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-",
        )

    _bteq_subprocess_output = ("BTEQ 15.10.01.00 Thu Sep 17 05:01:48 2020 PID: 11951",
            " ",
            "+---------+---------+---------+---------+---------+---------+---------+----",
            ".LOGON localhost/dbc,",
            " ",
            "*** Logon successfully completed.",
            "*** Teradata Database Release is 16.20.32.23",
            "*** Teradata Database Version is 16.20.32.23",
            "*** Transaction Semantics are BTET.",
            "*** Session Character Set Name is 'ASCII'.",
            " ",
            "*** Total elapsed time was 1 second.",
            " ",
            "+---------+---------+---------+---------+---------+---------+---------+----",
            ".SET WIDTH 65531;",
            "+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-",
            ".SET SESSION CHARSET 'ASCII';",
            "+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-",
            " ",
            "SELECT CURRENT_DATE;",
            "  ",
            "*** Query completed. One row found. One column returned.",
            "*** Total elapsed time was 1 second.",
            " ",
            "Date",
            "--------",
            "20/09/17",
            " ",
            "+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-",
            ".IF ERRORCODE <> 0 THEN .QUIT 0300;",
            "+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-",
            ".QUIT 0;",
            "*** You are now logged off from the DBC.",
            "*** Exiting BTEQ...",
            "*** RC (return code) = 0",)

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='ttu_default',
                conn_type='ttu',
                host='localhost',
                login='login',
                password='password'
            )
        )
        db.merge_conn(
            Connection(
                conn_id='ttu_default_bteq_with_params',
                conn_type='ttu',
                host='localhost_with_params',
                login='login',
                password='password',
                extra='{"bteq_output_width": 10000,"bteq_session_encoding": "UTF8","bteq_quit_zero": true}'
            )
        )
        db.merge_conn(
            Connection(
                conn_id='ttu_default_with_all_params',
                conn_type='ttu',
                host='localhost_with_all_params',
                login='login',
                password='password',
                extra='''{"bteq_output_width": 12345,
                        "bteq_session_encoding": "latin-1",
                        "bteq_quit_zero": true,
                        "ttu_log_folder": "/tmp/",
                        "ttu_max_sessions": 999,
                        "console_output_encoding": "latin1"}'''
            )
        )
    def test_get_conn(self):
        hook = TtuHook(ttu_conn_id='ttu_default_with_all_params')
        conn_dict = hook.get_conn()
        self.assertEqual(conn_dict,
            dict(
                login='login',
                password='password',
                host='localhost_with_all_params',
                ttu_log_folder='/tmp/',
                ttu_max_sessions=999,
                console_output_encoding='latin1',
                bteq_session_encoding='latin-1',
                bteq_output_width=12345,
                bteq_quit_zero=True,
                sp = None
                )
        )

    def test_prepare_bteq_script(self):
        # Given
        hook = TtuHook(ttu_conn_id='ttu_default')

        # When
        bteq_file = hook._prepare_bteq_script(self._simple_bteq,
                                        hook.get_conn()['host'],
                                        hook.get_conn()['login'],
                                        hook.get_conn()['password'],
                                        hook.get_conn()['bteq_output_width'],
                                        hook.get_conn()['bteq_session_encoding'],
                                        hook.get_conn()['bteq_quit_zero']
                                        )
        # Then
        expected_bteq_file = (".LOGON localhost/login,password;\n"
        ".SET WIDTH 65531;\n"
        ".SET SESSION CHARSET 'ASCII';\n"
        "SELECT CURRENT_DATE;\n"
        ".IF ERRORCODE <> 0 THEN .QUIT 0300;\n"
        ".LOGOFF;\n"
        ".EXIT;")
        self.assertEqual(expected_bteq_file, bteq_file)

    def test_prepare_bteq_script_with_params(self):
        # Given
        hook = TtuHook(ttu_conn_id='ttu_default_bteq_with_params')

        # When
        bteq_file = hook._prepare_bteq_script(self._simple_bteq,
                                        hook.get_conn()['host'],
                                        hook.get_conn()['login'],
                                        hook.get_conn()['password'],
                                        hook.get_conn()['bteq_output_width'],
                                        hook.get_conn()['bteq_session_encoding'],
                                        hook.get_conn()['bteq_quit_zero']
                                        )
        # Then
        expected_bteq_file = (".LOGON localhost_with_params/login,password;\n"
        ".SET WIDTH 10000;\n"
        ".SET SESSION CHARSET 'UTF8';\n"
        "SELECT CURRENT_DATE;\n"
        ".IF ERRORCODE <> 0 THEN .QUIT 0300;\n"
        ".QUIT 0;\n"
        ".LOGOFF;\n"
        ".EXIT;")
        self.assertEqual(expected_bteq_file, bteq_file)

    @patch('airflow.providers.teradata.hooks.ttu.subprocess.Popen')
    @patch('airflow.providers.teradata.hooks.ttu.TemporaryDirectory')
    @patch('airflow.providers.teradata.hooks.ttu.NamedTemporaryFile')
    def test_execute_bteq_runcmd(self, mock_tmpfile, mock_tmpdir, mock_popen):
        # Given
        mock_subprocess = MockSubProcess()
        mock_subprocess.returncode = 0
        mock_popen.return_value = mock_subprocess
        mock_tmpdir.return_value.__enter__.return_value = '/tmp/airflowtmp_ttu_bteq'
        mock_tmpfile.return_value.__enter__.return_value.name = 'test.bteq'
        # When
        hook = TtuHook(ttu_conn_id='ttu_default')
        hook.execute_bteq(bteq="")

        # Then
        mock_popen.assert_called_with(['bteq'],
            stdin=mock.ANY,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd='/tmp/airflowtmp_ttu_bteq',
            preexec_fn=mock.ANY
        )

    @patch('airflow.providers.teradata.hooks.ttu.subprocess.Popen')
    @patch('airflow.providers.teradata.hooks.ttu.TemporaryDirectory')
    @patch('airflow.providers.teradata.hooks.ttu.NamedTemporaryFile')
    def test_execute_bteq_runcmd_error_raise(self, mock_tmpfile, mock_tmpdir, mock_popen):
        # Given
        mock_subprocess = MockSubProcess(output=self._bteq_failure_subprocess_output)
        mock_subprocess.returncode = 311
        mock_popen.return_value = mock_subprocess
        mock_tmpdir.return_value.__enter__.return_value = '/tmp/airflowtmp_ttu_bteq'
        mock_tmpfile.return_value.__enter__.return_value.name = 'test.bteq'

        # When
        hook = TtuHook(ttu_conn_id='ttu_default')

        # Then
        with self.assertRaises(AirflowException) as cm:
            hook.execute_bteq(bteq="")
        msg = ("BTEQ command exited with return code 311 because of "
        "*** Failure 3706 Syntax error: expected something between '(' and the string 'test'")
        self.assertEqual(str(cm.exception), msg)

    @patch('airflow.providers.teradata.hooks.ttu.subprocess.Popen')
    @patch('airflow.providers.teradata.hooks.ttu.TemporaryDirectory')
    @patch('airflow.providers.teradata.hooks.ttu.NamedTemporaryFile')
    def test_execute_bteq_runcmd_error_noraise(self, mock_tmpfile, mock_tmpdir, mock_popen):
        # Givens
        mock_subprocess = MockSubProcess(output=self._bteq_error_no_failure_subprocess_output)
        mock_subprocess.returncode = 0
        mock_popen.return_value = mock_subprocess
        mock_tmpdir.return_value.__enter__.return_value = '/tmp/airflowtmp_ttu_bteq'
        mock_tmpfile.return_value.__enter__.return_value.name = 'test.bteq'

        # When
        hook = TtuHook(ttu_conn_id='ttu_default')

        # Then
        with self.assertLogs(level="INFO") as cm:
            hook.execute_bteq(bteq="")
        self.assertEqual(
            "INFO:airflow.providers.teradata.hooks.ttu.TtuHook:BTEQ command exited with return code 0",
            cm.output[-1],
        )

    @patch('airflow.providers.teradata.hooks.ttu.subprocess.Popen')
    @patch('airflow.providers.teradata.hooks.ttu.TemporaryDirectory')
    @patch('airflow.providers.teradata.hooks.ttu.NamedTemporaryFile')
    def test_execute_bteq_runcmd_return_last_line(self, mock_tmpfile, mock_tmpdir, mock_popen):
        # Givens
        mock_subprocess = MockSubProcess(output=self._bteq_subprocess_output)
        mock_subprocess.returncode = 0
        mock_popen.return_value = mock_subprocess
        mock_tmpdir.return_value.__enter__.return_value = '/tmp/airflowtmp_ttu_bteq'
        mock_tmpfile.return_value.__enter__.return_value.name = 'test.bteq'

        # When
        hook = TtuHook(ttu_conn_id='ttu_default')

        # Then
        res = hook.execute_bteq(bteq="", xcom_push_flag=True)
        self.assertEqual(
            "*** RC (return code) = 0",
            res,
        )
    
    #def test_execute_bteq_runcmd_log_stdout():
