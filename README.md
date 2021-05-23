# Project 4: Data Lake

## Introduction

Sparkify, a music streaming startup, has grown their user base and song database. In order to cope with the increasing load, they have decided to process the data in the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app. Due to the growing size of the data, Sparkify has decided to transform their data using an on-demand EMR Spark cluster and store the transformed on a cost-effective S3 blob storage. The resulting files will then be used to perform analysis of the activity of their user base.

## Goal

## Source Data

There are two datasets that are stored in S3 bucket `udacity-dend`.

- Song data: `s3://udacity-dend/song_data`
    - Number of files: 14'896
    - Each file contains one record
- Log data: `s3://udacity-dend/log_data`
    - Number of files: 30
    - Each files contains multiple JSON objects that represent a record, separated by a newline.

### Song Dataset

The song dataset is partitioned by the first three letters of each song's track ID. For example:

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

The content of a song dataset looks like this:

```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

Log datasets are partitioned by year and month:

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

Here is a screenshot of a log file:

![Log Dataset](images/source_log_data.png)

## Data Model

## Project Structure

## Usage

## Example Queries

use redshift external table
redshift spectrum
athena
emr notebook