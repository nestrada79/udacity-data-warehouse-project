import configparser
import os
from dotenv import load_dotenv
load_dotenv()

config = configparser.ConfigParser()
config.read('dwh.cfg')

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR,
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         FLOAT,
    year             INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id   INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time    TIMESTAMP NOT NULL SORTKEY,
    user_id       VARCHAR NOT NULL,
    level         VARCHAR,
    song_id       VARCHAR,
    artist_id     VARCHAR,
    session_id    INTEGER,
    location      VARCHAR,
    user_agent    VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     VARCHAR PRIMARY KEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      VARCHAR,
    level       VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id    VARCHAR PRIMARY KEY,
    title      VARCHAR,
    artist_id  VARCHAR,
    year       INTEGER,
    duration   FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id  VARCHAR PRIMARY KEY,
    name       VARCHAR,
    location   VARCHAR,
    latitude   FLOAT,
    longitude  FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
);
""")

staging_events_copy = f"""
COPY staging_events
FROM '{config['S3']['LOG_DATA']}'
CREDENTIALS 'aws_access_key_id={AWS_KEY};aws_secret_access_key={AWS_SECRET}'
FORMAT AS JSON '{config['S3']['LOG_JSONPATH']}'
REGION '{config['REGION']['NAME']}';
"""

staging_songs_copy = f"""
COPY staging_songs
FROM '{config['S3']['SONG_DATA']}'
CREDENTIALS 'aws_access_key_id={AWS_KEY};aws_secret_access_key={AWS_SECRET}'
FORMAT AS JSON 'auto'
REGION '{config['REGION']['NAME']}';
"""


songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id,
    session_id, location, user_agent
)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'       AS start_time,
    se.userId                                                   AS user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
LEFT JOIN staging_songs ss
       ON se.song = ss.title
      AND se.artist = ss.artist_name
      AND ABS(se.length - ss.duration) < 2
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    ts_timestamp AS start_time,
    EXTRACT(hour FROM ts_timestamp),
    EXTRACT(day FROM ts_timestamp),
    EXTRACT(week FROM ts_timestamp),
    EXTRACT(month FROM ts_timestamp),
    EXTRACT(year FROM ts_timestamp),
    EXTRACT(weekday FROM ts_timestamp)
FROM (
    SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS ts_timestamp
    FROM staging_events
);
""")

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
    songplay_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert
]


