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
    CREATE TABLE staging_events(
        artist text,
        auth text,
        firstname text,
        gender text,
        iteminsession int,
        lastname text,
        length numeric,
        level text,
        location text,
        method text,
        page text,
        registration numeric,
        sessionid int,
        song text,
        status int,
        ts bigint,
        useragent text,
        user_id int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_song int,
        artist_id text,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location text,
        artist_name text,
        song_id text,
        title text,
        duration numeric,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level text,
        song_id text,
        artist_id text,
        session_id int,
        location text,
        user_agent text
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id int NOT NULL PRIMARY KEY,
        firstname text,
        lastname text,
        gender text,
        level text
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id text NOT NULL PRIMARY KEY,
        title text,
        artist_id text,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id text NOT NULL PRIMARY KEY,
        name text,
        location text,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time timestamp NOT NULL PRIMARY KEY,
        hour int,
        day int,
        week int,
        month text,
        year int,
        weekday text
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    ima_role {}
    json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    ima_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level,song_id, artist_id, session_id, location,user_agent)
    SELECT events.start_time, events.user_id, events.level, songs.song_id, songs.artist_id, events.session_id, events.location, events.user_agent
    FROM ( SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page = 'Nextsong') events
    LEFT JOIN stating_songs songs
    ON events.song=songs.title
    AND events.artist=songs.artist_name
    AND events.length=songs.duration


    )
""")

user_table_insert = ("""
    INSERT INTO users (user_id, firstname, lastname,level, gender)
    SELECT DISTINCT user_id, firstname, lastname,level,gender
    FROM stating_events
    WHERE page='Nextsong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, duration)
    SELECT DISTINCT song_id, title, artist_id, duration
    FROM stating_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM stating_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
           extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
