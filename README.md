# Airflow Udacity Project

## Table of contents
* [Intro](#Intro)
* [Motivation](#Motivation)
* [Files](#Files)
* [Technologies](#Technologies)
* [Install](#Install)
* [Acknowledgement](#Acknowledgement)


## Intro:

## Motivation:

## Files:
Udac_example_dag.py - This is an Airflow file which contains the setup for the dag and tasks along with task worflow.
stage_redshift.py - This file is used to create a class and methods for accessing files from S3 and creating/staging onto a Redshift table.
load_dimensions.py - This file is used to create a class and methods for insert/upserting into dimensions tables in Redshfit.
load_fact.py - This file is used to create a class and methods for insert/upserting into the fact table in Redshfit.
sql-queries.py - This file contains the queries that are used by the methods in the load_dimensions and load_fact files.
data_quality.p.y - Simple data check file, which checks counts and ensures that tables are not empty.

## Technologies:


## Install:


### Resources:
