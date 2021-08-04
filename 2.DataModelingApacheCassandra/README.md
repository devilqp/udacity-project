
# Data Engineer Nanodegree - Project 2: Data modeling with Apache Cassandra

- [Data Engineer Nanodegree - Project 2: Data modeling with Apache Cassandra](#data-engineer-nanodegree---project-1-data-model-with-postgresql)
  - [Overview](#overview)
  - [Dataset](#dataset)

## Overview
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Dataset
The project is based on one dataset: `event_data`.
The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-01-events.csv
event_data/2018-11-02-events.csv

```