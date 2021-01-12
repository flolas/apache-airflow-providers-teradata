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
def get_provider_info():
    return {
        'package-name': 'apache-airflow-providers-teradata',
        'name': 'Teradata',
        'description': '`Teradata Tools and Utilities (TTU) <https://downloads.teradata.com/download/tools/teradata-tools-and-utilities-linux-installation-package-0>`__\n',
        'versions': ['1.0.0'],
        'integrations':[
            {
                'integration-name': 'Teradata Tools and Utilities (TTU)',
                'external-doc-url': 'https://downloads.teradata.com/download/tools/teradata-tools-and-utilities-linux-installation-package-0',
                'tags': ['ttu'],
            }

        ],
        'hooks':[
            {
                'integration-name': 'Teradata Ttu',
                'python-modules': ['airflow.providers.teradata.hooks.ttu'],
            }
        ],
        'operators':[
            {
                'integration-name': 'Teradata BTEQ',
                'python-modules': ['airflow.providers.teradata.operators.bteq'],
            },
            {
                'integration-name': 'Teradata FastExport',
                'python-modules': ['airflow.providers.teradata.operators.fastexport'],
            },
            {
                'integration-name': 'Teradata FastLoad',
                'python-modules': ['airflow.providers.teradata.operators.fastload'],
            }
        ],
        'hook-class-names': ['airflow.providers.teradata.hooks.ttu.TtuHook']
    }