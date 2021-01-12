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


import unittest
from unittest import mock
from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.teradata.hooks.ttu import TtuHook

class TestTtuHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(host='host')

        class UnitTestTttuHook(TtuHook):
            conn_name_attr = 'ttu_conn_id'

        self.db_hook = UnitTestTttuHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @patch('airflow.providers.teradata.hooks.ttu.TtuHook')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_connection()
        mock_connect.assert_called()
