# Import necessary libraries
import configparser

# Read configuration file
config = configparser.ConfigParser()
config.read("dwh.cfg")

'''

SQL queries to create tables in Redshift

Global Variables:

LOG_DATA: stores the log data S3 bucket path
LOG_PATH: stores the log path S3 bucket for json format
SONG_DATA: stores the songs data S3 bucket path
IAM_ROLE: stores IAM role and ARN details for the Amazon Redshift database

'''


# Define Global variables from the configuration file
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

# Drop table queries
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# Create staging events table
staging_events_table_create = ("""
CREATE TABLE staging_events (
    artist VARCHAR(450),
    auth VARCHAR(450),
    firstName VARCHAR(450),
    gender VARCHAR(50),
    ItemInSession INTEGER,
    lastName VARCHAR(450),
    length FLOAT,
    level VARCHAR(450),
    location VARCHAR(450),
    method VARCHAR(450),
    page VARCHAR(450),
    registration VARCHAR(450),
    sessionId INTEGER,
    song VARCHAR(65535),
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR(450),
    userId INTEGER
)
""")

# Create staging songs table
staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id VARCHAR(256),
    artist_id VARCHAR(256),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(450),
    artist_name VARCHAR(65535),
    duration FLOAT,
    num_songs INT,
    title VARCHAR(65535),
    year INT
)
""")

# Create songplays table
songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR(500) NOT NULL,
    level VARCHAR(50),
    song_id VARCHAR(500) NOT NULL,
    artist_id VARCHAR(500) NOT NULL,
    session_id VARCHAR(500),
    location VARCHAR(500),
    user_agent VARCHAR(500)
)
    
""")

# Create users table
user_table_create = ("""
CREATE TABLE users
(
    user_id VARCHAR(500) PRIMARY KEY NOT NULL,
    first_name VARCHAR(500),
    last_name VARCHAR(500),
    gender VARCHAR(50),
    level VARCHAR(500)
)
""")

# Create songs table
song_table_create = ("""
CREATE TABLE songs
(
    song_id VARCHAR(500) PRIMARY KEY NOT NULL,
    title VARCHAR(500) NOT NULL,
    artist_id VARCHAR(500) NOT NULL,
    duration DECIMAL NOT NULL,
    year INTEGER
)
""")

# Create artists table
artist_table_create = ("""
CREATE TABLE artists
(
    artist_id VARCHAR(500) PRIMARY KEY NOT NULL,
    name VARCHAR(500),
    latitude DECIMAL,
    longitude DECIMAL,
    location VARCHAR(500)
)
""")

# Create time table
time_table_create = ("""
CREATE TABLE time
(
    start_time TIMESTAMP PRIMARY KEY NOT NULL,
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER,
    weekday INTEGER
)
""")

# COPY queries for data loading
staging_events_copy = (f"""
    COPY staging_events FROM {LOG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-east-1'
    FORMAT AS JSON {LOG_PATH}
    TIMEFORMAT AS 'epochmillisecs'
""")

staging_songs_copy = (f"""
    COPY staging_songs FROM {SONG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-east-1'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL
""")

# INSERT queries
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second' AS start_time,
    se.userid,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionid,
    se.location,
    se.useragent
FROM staging_events se
JOIN staging_songs ss ON ss.artist_name = se.artist AND se.page = 'NextSong' AND se.song = ss.title
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    se3.userid,
    se3.firstname,
    se3.lastname,
    se3.gender,
    se3.level
FROM
    staging_events se3
JOIN (
    SELECT
        userid,
        MAX(ts) AS max_time_stamp
    FROM
        staging_events
    WHERE
        page = 'NextSong'
    GROUP BY
        userid
) se2 ON se3.userid = se2.userid AND se3.ts = se2.max_time_stamp
WHERE
    se3.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM
    staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id , name, location, latitude, longitude)
SELECT DISTINCT 
    artist_id
    ,artist_name
    ,artist_location
    ,artist_latitude
    ,artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT (hour FROM start_time),
    EXTRACT (day FROM start_time),
    EXTRACT (week FROM start_time),
    EXTRACT (month FROM start_time),
    EXTRACT (year FROM start_time),
    EXTRACT (weekday FROM start_time)
FROM staging_events
WHERE ts IS NOT NULL;
""")

# List of queries for creating tables
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]

# List of queries for dropping tables
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

# List of queries for copying data into tables
copy_table_queries = [staging_events_copy, staging_songs_copy]

# List of queries for inserting data into tables 
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]