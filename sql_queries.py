import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

pro_dwh_schema_drop ="DROP schema IF EXISTS {} CASCADE"
staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users cascade"
song_table_drop = "DROP table IF EXISTS songs cascade"
artist_table_drop = "DROP table IF EXISTS artists cascade"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

pro_dwh_schema_create = """
    CREATE SCHEMA IF NOT EXISTS {};
"""

staging_songs_table_create= ("""
    SET search_path TO {};
    CREATE TABLE "staging_songs"(
        "artist_id" varchar,
        "artist_latitude" float8,
        "artist_location" varchar,
        "artist_longitude" float8,
        "artist_name" varchar,
        "duration" float8,
        "num_songs" float8,
        "song_id" varchar,
        "title" varchar,
        "year" float8
    );
""")

staging_events_table_create = ("""
    SET search_path TO {};
    CREATE TABLE "staging_events"(
        "artist" varchar,
        "auth" varchar,
        "firstName" varchar,
        "gender" character,
        "itemInSession" integer,
        "lastName" varchar,
        "length" float8,
        "level" varchar,
        "location" varchar,
        "method" varchar,
        "page" varchar,
        "registration" float8,
        "sessionId" integer,
        "song" varchar,
        "status" integer,
        "ts" BIGINT,
        "userAgent" varchar,
        "userId" integer
    );
""")

songplay_table_create = ("""
    SET search_path TO {};
    CREATE TABLE songplays (
        songplay_id integer IDENTITY(0,1) PRIMARY KEY NOT NULL,
        start_time TIMESTAMP NOT NULL sortkey,
        user_id integer NOT NULL distkey,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id integer,
        location varchar,
        user_agent varchar,
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
        FOREIGN KEY (song_id) REFERENCES songs(song_id)
    );
""")

user_table_create = ("""
    SET search_path TO {};
    CREATE TABLE users (
        user_id integer PRIMARY KEY NOT NULL sortkey distkey,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    );
""")

song_table_create = ("""
    SET search_path TO {};
    CREATE TABLE songs (
        song_id varchar PRIMARY KEY NOT NULL sortkey distkey,
        title varchar,
        artist_id varchar,
        year integer,
        duration float8
    );
""")

artist_table_create = ("""
    SET search_path TO {};
    CREATE TABLE artists (
        artist_id varchar PRIMARY KEY NOT NULL sortkey distkey,
        name varchar,
        location varchar,
        latitude FLOAT8,
        longitude FLOAT8
    );
""")

time_table_create = ("""
    SET search_path TO {};
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY NOT NULL sortkey distkey,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer
    );
""")

# STAGING TABLES
staging_events_copy = ("""
    SET search_path TO {};
    copy staging_songs 
    from {}
    iam_role {} json 'auto'
""").format('{}',config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
    SET search_path TO {};
    copy staging_events 
    from {}
    iam_role {} json {}
""").format('{}', config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get( 'S3','LOG_JSONPATH'))

# FINAL TABLES

songplay_table_insert = ("""
    SET search_path TO {};
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS start_time, e.userid, e.level, s.song_id, s.artist_id, e.sessionid, e.location , e.userAgent
    FROM staging_events AS e LEFT JOIN staging_songs AS s ON (s.artist_name = e.artist)
    AND (s.duration = e.length) AND(s.title = e.song)
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    SET search_path TO {};
    INSERT INTO users( user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events AS t1
    WHERE ts = (
        SELECT max(ts) FROM staging_events
        WHERE page = 'NextSong'
        AND userId = t1.userId
        GROUP BY userId
    )
""")

song_table_insert = ("""
    SET search_path TO {};
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
    SET search_path TO {};
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs 
""")

time_table_insert = ("""
    SET search_path TO {};
    INSERT INTO time(start_time, hour, day, week, month, year)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time,
    date_part('hour', start_time) AS hour,
    date_part('day', start_time) AS day,
    date_part('week', start_time) AS week,
    date_part('month', start_time) AS month,
    date_part('year', start_time) AS year
    FROM staging_events
""")

# QUERY LISTS

create_schema_queries = [pro_dwh_schema_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
drop_schema_queries = [pro_dwh_schema_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
