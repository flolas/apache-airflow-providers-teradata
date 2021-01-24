#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_namespace_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

setup(
    author="Felipe Lolas",
    author_email='flolas@icloud.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Teradata Tools and Utils wrapper for Apache Airflow 2.0/1.1x.",
    install_requires=['apache-airflow>=2.0.0a0',],
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords=['airflow', 'teradata'],
    name='apache-airflow-providers-teradata',
    packages=find_namespace_packages(include=['airflow.providers.teradata', 'airflow.providers.teradata.*']),
    setup_requires=['setuptools', 'wheel'],
    url='https://github.com/flolas/apache_airflow_providers_teradata',
    version='1.0.3',
    zip_safe=False,
)
