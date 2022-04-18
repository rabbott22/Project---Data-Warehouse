import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA        = config.get('S3','LOG_DATA')  
LOG_JSONPATH    = config.get('S3','LOG_JSONPATH')
SONG_JSONPATH   = config.get('S3','SONG_JSONPATH')
SONG_DATA_MAN   = config.get('S3','SONG_DATA_MAN')
DWH_ROLE_ARN    = config.get('IAM_ROLE','DWH_ROLE_ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# DROP SCHEMAS

stage_schema_drop = "DROP SCHEMA IF EXISTS stage CASCADE;"
dwh_schema_drop = "DROP SCHEMA IF EXISTS dwh CASCADE;"

# CREATE SCHEMAS

stage_schema_create = "CREATE SCHEMA IF NOT EXISTS stage;"
dwh_schema_create = "CREATE SCHEMA IF NOT EXISTS dwh;"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
    eventId bigint IDENTITY PRIMARY KEY,
    artist varchar,
    auth varchar(15),
    firstName varchar(15),
    gender char(1),
    itemInSession smallint,
    lastName varchar(20),
    length decimal(10,5),
    level char(4),
    location varchar(55),
    method char(7),
    page char(20),
    registration decimal(15,1),
    sessionId int,
    song varchar,
    status smallint,
    ts bigint,
    userAgent varchar,
    userId int
    ) diststyle all;""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    songId bigint IDENTITY PRIMARY KEY      DISTKEY,
    artist_id varchar(25),
    artist_latitude decimal(8,6),
    artist_location varchar,
    artist_longitude decimal(9,6),
    artist_name varchar,
    duration decimal(10,5),
    num_songs smallint,
    song_id varchar(25),
    title varchar,
    year smallint
    );""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
    songplay_id bigint IDENTITY PRIMARY KEY,
    start_time timestamp REFERENCES time (start_time) NOT NULL,
    user_id int REFERENCES users (user_id) NOT NULL,
    level char(4),
    song_id varchar REFERENCES song (song_id),
    artist_id varchar REFERENCES artist (artist_id),
    session_id int,
    location varchar,
    user_agent varchar
    );""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    first_name varchar(15),
    last_name varchar(20),
    gender char(1),
    level char(4)
    );""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (
    song_id varchar(25) PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar(25) REFERENCES artist (artist_id),
    year smallint,
    duration decimal(10,5) NOT NULL
    );""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
    artist_id varchar(25) PRIMARY KEY,
    name varchar NOT NULL,
    location varchar(55),
    latitude decimal(8,6),
    longitude decimal(9,6));""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint
    );""")

# STAGING TABLES

staging_events_copy = ("""copy stage.staging_events from '{}/2018/11/2018'
    credentials 'aws_iam_role={}'
    json '{}'
    region 'us-west-2';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy stage.staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json '{}'
    manifest
    region 'us-west-2';
""").format(SONG_DATA_MAN, DWH_ROLE_ARN, SONG_JSONPATH)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET level = EXCLUDED.level""")

song_table_insert = ("""SELECT song_id, title, artist_id, year, duration 
    INTO dwh.song song_id, title, artist_id, year, duration 
    VALUES (%s, %s, %s, %s, %s) 
    FROM stage.staging_songs
    ON CONFLICT ON CONSTRAINT song_pkey DO NOTHING""")

artist_table_insert = ("""SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    INTO dwh.artist (artist_id, name, location, latitude, longitude) 
    VALUES (%s, %s, %s, %s, %s)
    FROM stage.staging_songs
    ON CONFLICT ON CONSTRAINT artist_pkey DO NOTHING""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT time_pkey DO NOTHING""")

# QUERY LISTS

create_schema_queries = [stage_schema_create, dwh_schema_create]
drop_schema_queries = [stage_schema_drop, dwh_schema_drop]
create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]
create_table_queries = [artist_table_create, song_table_create, user_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert, time_table_insert, user_table_insert, songplay_table_insert]
