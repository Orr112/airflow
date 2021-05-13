# Airflow Udacity Project

## Table of contents
* [Intro](#Intro)
* [Motivation](#Motivation)
* [Files](#Files)
* [Technologies](#Technologies)
* [Install](#Install)
* [Acknowledgement](#Acknowledgement)


## Intro:
Airflow is an open-source platform used to create, schedule, and monitor workflows.  Users can define a collection of tasks and organize them so that relationships and dependencies are accounted for programmatically.  These collections of tasks are referred to as a DAG (Directed Acyclic Graph), which is simply a graph with nodes that contains directed edges and does not contiain any cycles.  Here is a simple machine learning dag example:

![alt text](https://marclamberti.com/wp-content/uploads/Screenshot-2021-03-02-at-22.43.28.png)

Image provide by Marc Lamberti via blog (URL is referrenced below in Acknowledgment section).




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
