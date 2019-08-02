import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (artist TEXT,
                                           auth TEXT,
                                           firstName TEXT,
                                           gender TEXT,
                                           iteminSession INT,
                                           lastName TEXT,
                                           length FLOAT,
                                           level TEXT,
                                           location TEXT,
                                           method TEXT,
                                           page TEXT,
                                           registration TEXT,
                                           sessionId INT,
                                           song TEXT,
                                           status INT,
                                           ts INT8,
                                           userAgent TEXT,
                                           userId INT)

""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (num_songs INT,
                                          artist_id TEXT,
                                          artist_latitude FLOAT,
                                          artist_longitude FLOAT,
                                          artist_location TEXT,
                                          artist_name TEXT,
                                          song_id TEXT,
                                          title TEXT,
                                          duration FLOAT,
                                          year INT)

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(0,1) PRIMARY KEY, --auto-increasement
                                      start_time INT8 NOT NULL,
                                      user_id INT NOT NULL,
                                      level TEXT,
                                      song_id TEXT, -- as we are using subset of songs data, so song_id and artist_id might be NULL
                                      artist_id TEXT NOT NULL,
                                      session_id INT, 
                                      location TEXT,
                                      user_agent TEXT)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY,
                                  first_name TEXT,
                                  last_name TEXT NOT NULL,
                                  gender TEXT,
                                  level TEXT)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id TEXT PRIMARY KEY,
                                  title TEXT,
                                  artist_id TEXT,
                                  year INT, 
                                  duration FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id TEXT PRIMARY KEY,
                                    name TEXT,
                                    location TEXT,
                                    latitude FLOAT,
                                    longitude FLOAT)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time INT8 PRIMARY KEY,
                                 hour INT,
                                 day INT,
                                 week INT,
                                 month INT,
                                 year INT,
                                 weekday INT)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json '{}'
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto' truncatecolumns -- truncate if strings are too long
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts AS start_time,
        e.userId AS user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId AS session_id,
        e.location,
        e.userAgent AS user_agent
        
FROM staging_events e JOIN staging_songs s ON e.song = s.title  
WHERE e.userId IS NOT NULL 
        AND e.ts IS NOT NULL
        AND e.page = 'NextSong'
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT e.userId AS user_id,
        e.firstName AS first_name,
        e.lastName AS last_name,
        e.gender,
        e.level

FROM staging_events e
WHERE e.userId IS NOT NULL
        AND e.page = 'NextSong'
""")


song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT s.song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration

FROM staging_songs s
""")


artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT s.artist_id,
        s.artist_name AS name,
        s.artist_location AS location,
        s.artist_latitude AS latitude,
        s.artist_longitude AS longitude

FROM staging_songs s
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT e.ts AS start_time,
        date_part('hour', e.timestamp) AS hour,
        date_part('day', e.timestamp) AS day,
        date_part('week', e.timestamp) AS week,
        date_part('month', e.timestamp) AS month,
        date_part('year', e.timestamp) AS year,
        date_part('weekday', e.timestamp) AS weekday

FROM (
        SELECT e2.ts, 
        TIMESTAMP WITHOUT TIME ZONE 'epoch' + (e2.ts::bigint::float / 1000) * INTERVAL '1 second' AS timestamp
        FROM staging_events e2
) e
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
