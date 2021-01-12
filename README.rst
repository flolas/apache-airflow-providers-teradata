apache-airflow-providers-teradata
=================================

.. figure:: https://github.com/flolas/apache-airflow-providers-teradata/workflows/Upload%20Python%20Package/badge.svg
   :alt: Upload Python Package

   Upload Python Package
Release: 2020.9.30

**Table of contents**

-  `Backport package <#backport-package>`__
-  `Installation <#installation>`__
-  `Compatibility <#compatibility>`__
-  `PIP requirements <#pip-requirements>`__
-  `Cross provider package
   dependencies <#cross-provider-package-dependencies>`__
-  `Provider class summary <#provider-classes-summary>`__

   -  `Operators <#operators>`__

      -  `Moved operators <#moved-operators>`__

   -  `Hooks <#hooks>`__

      -  `Moved hooks <#moved-hooks>`__

-  `Releases <#releases>`__

   -  `Release 2020.9.30 <#release-2020930>`__

Backport package
----------------

This is a backport providers package for ``teradata`` provider. All
classes for this provider package are in ``airflow.providers.teradata``
python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.\* continues to support Python 2.7+ - you need to
upgrade python to 3.6+ if you want to use this backport package.

Installation
------------

You can install this package on top of an existing airflow 1.10.\*
installation via
``pip install apache-airflow-backport-providers-teradata``

Compatibility
-------------

For full compatibility and test status of the backport packages check
`Airflow Backport Package
Compatibility <https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series>`__

PIP requirements
----------------

None

Cross provider package dependencies
-----------------------------------

None

Third-party requirements
------------------------

This package uses bteq, mload and tpy propietary binaries from Teradata

-  `Teradata Tools and
   Utilities <https://downloads.teradata.com/download/tools/teradata-tools-and-utilities-linux-installation-package-0>`__
   (Release 17.00.15.00)

For installing and configuring Teradata libraries, use the documents
provided by `Teradata Information <http://www.info.teradata.com/>`__ or
by engaging with the `Teradata
Community <https://community.teradata.com/>`__.

We are not affiliated, associated, authorized, endorsed by, or in any
way officially connected with the Teradata Operations, Inc, or any of
its subsidiaries or its affiliates.

Provider classes summary
========================

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for
the ``teradata`` provider are in the ``airflow.providers.teradata``
package. You can read more about the naming conventions used in `Naming
conventions for provider
packages <https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages>`__

Operators
---------

New operators
~~~~~~~~~~~~~

\| New Airflow 2.0 operators: ``airflow.providers.teradata`` package \|
\|:--------------------------------------------------------------------------------------------------------------------------------------------------\|
\|
`operators.teradata.BteqOperator <https://github.com/apache/airflow/blob/master/airflow/providers/teradata/operators/bteq.py>`__
\| \|
`operators.teradata.FastLoadOperator <https://github.com/apache/airflow/blob/master/airflow/providers/teradata/operators/fastload.py>`__
\| \|
`operators.teradata.FastExportOperator <https://github.com/apache/airflow/blob/master/airflow/providers/teradata/operators/fastexport.py>`__
\|

Hooks
-----

New hooks
~~~~~~~~~

\| New Airflow 2.0 operators: ``airflow.providers.teradata`` package \|
\|:--------------------------------------------------------------------------------------------------------------------------------------------------\|
\|
`hooks.teradata.TtuHook <https://github.com/apache/airflow/blob/master/airflow/providers/teradata/hooks/ttu.py>`__
\|

Releases
--------

Release 2020.9.30
~~~~~~~~~~~~~~~~~

+-----------------------------------------------------------------------------------------------------+--------------+-----------------------------------------------+
| Commit                                                                                              | Committed    | Subject                                       |
+=====================================================================================================+==============+===============================================+
| `12af6a080 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`__   | 2020-09-30   | new teradata provider and hooks/ops (#XXXX)   |
+-----------------------------------------------------------------------------------------------------+--------------+-----------------------------------------------+

