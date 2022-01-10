# Data Engineer Nanodegree - Project 4: Data Lakes Spark

- [Data Engineer Nanodegree - Project 4: Data Lakes Spark](#data-engineer-nanodegree---project-1-data-model-with-postgresql)
  - [Overview](#overview)
  - [How to run](#how-to-run)
    - [1. Config file](#1-config-file)
    - [2. Requirements](#2-requirements)
    - [3. ETL](#3-etl)

## Overview

This project combines song listen log files with song metadata to facilitate analytics. JSON data is copied from an S3 bucket and processed using Apached Spark with the Python API. The data is organized into a star schema with fact and dimension tables. Analytics queries on the ` songplays` fact table are straightforward, and additional fields can be easily accessed in the four dimension tables `users`, `songs`, `artists`, and `time`. A star schema is suitable for this application since denormalization is easy, queries can be kept simple, and aggregations are fast.

## How to run

### 1. Config file
Rename the `dl.txt` file in `dl.cfg` and complete it with the correct informations.
### 2. Requirements
Be sure to have all requirements satisfied executing:
```
pip install -r requirements.txt
```
### 3. ETL
Now, you are ready to run the etl process executing the `etl.py` script.