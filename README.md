# apache-airflow-providers-teradata

![Upload Python Package](https://github.com/flolas/apache-airflow-providers-teradata/workflows/Upload%20Python%20Package/badge.svg)

Release: 2021.01.11

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-providers-teradata`

## PIP requirements
None

## Cross provider package dependencies
None

## Third-party requirements
This package uses BTEQ, MLOAD and TPT propietary binaries from @Teradata

* [Teradata Tools and Utilities](https://downloads.teradata.com/download/tools/teradata-tools-and-utilities-linux-installation-package-0) (Release 17.00.15.00)

For installing and configuring Teradata libraries, use the documents provided by [Teradata Information](http://www.info.teradata.com/) or by engaging with the [Teradata Community](https://community.teradata.com/).

We are not affiliated, associated, authorized, endorsed by, or in any way officially connected with the Teradata Operations, Inc, or any of its subsidiaries or its affiliates.

## Operators

| Operators: `airflow.providers.teradata` package                                                                                |
|:--------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.bteq.BteqOperator](https://github.com/flolas/apache-airflow-providers-teradata/blob/main/airflow/providers/teradata/operators/bteq.py) |
| [operators.fastload.FastLoadOperator](https://github.com/flolas/apache-airflow-providers-teradata/blob/main/airflow/providers/teradata/operators/fastload.py) |
| [operators.fastexport.FastExportOperator](https://github.com/flolas/apache-airflow-providers-teradata/blob/main/airflow/providers/teradata/operators/fastexport.py) |

## Hooks

| Hooks: `airflow.providers.teradata` package                                                                                |
|:--------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.ttu.TtuHook](https://github.com/flolas/apache-airflow-providers-teradata/blob/main/airflow/providers/teradata/hooks/ttu.py) |
