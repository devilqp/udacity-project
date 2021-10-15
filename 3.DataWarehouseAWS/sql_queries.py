import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_event (
    staging_event_id bigint identity(0,1) PRIMARY KEY,
    artist text,
    auth text,
    firstName text,
    gender char,
    itemInSession integer,
    lastName text,
    length numeric,
    level varchar(10),
    location text,
    method varchar(10),
    page text,
    registration bigint,
    sessionId integer,
    song text,
    status integer,
    ts timestamp,
    userAgent text,
    userId integer
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_song (
    staging_song_id bigint identity(0,1) PRIMARY KEY,
    num_songs integer,
    artist_id text,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar(100),
    artist_name varchar(100),
    song_id text,
    title text,
    duration numeric,
    year integer
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint identity(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar
    );
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
    );
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year int,
    duration numeric
    );
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric
    );
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
    );
"""

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_event FROM {s3_file_path}
    CREDENTIALS 'aws_iam_role={role_arn}'
    json {json_path}
    timeformat 'epochmillisecs';
"""
).format(
    s3_file_path=config.get("S3", "LOG_DATA"),
    role_arn=config.get("IAM_ROLE", "ARN"),
    json_path=config.get("S3", "LOG_JSONPATH"),
)

staging_songs_copy = (
    """
    COPY staging_song FROM {s3_file_path}
    CREDENTIALS 'aws_iam_role={role_arn}'
    json 'auto' TRUNCATECOLUMNS;
"""
).format(s3_file_path=config.get("S3", "SONG_DATA"), role_arn=config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(select ts, userId, level, song_id, artist_id, sessionId, location, userAgent
from staging_event join staging_song  on (song = title and artist_name = artist))
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
(select distinct(userId), firstName, lastName, gender, level
from staging_event
where userId IS NOT NULL)
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
(select distinct(song_id), title, artist_id, year, duration
from staging_song
where song_id IS NOT NULL)
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
(select distinct(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
from staging_song
where artist_id IS NOT NULL)
"""


time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
(select distinct(ts), EXTRACT(hour from ts), EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(month from ts), EXTRACT(year from ts), EXTRACT(weekday from ts)
from staging_event)
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert,
]