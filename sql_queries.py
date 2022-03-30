import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (eventId bigint IDENTITY PRIMARY KEY, artist varchar, firstName varchar(15), gender char(1), lastName varchar(15), length decimal(10,5), level char(4), location varchar(55), sessionId int, song varchar, ts bigint);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (songId bigint IDENTITY PRIMARY KEY, num_songs int, artist_id varchar(25), artist_latitude decimal(8,6), artist_longitude decimal(9,6), artist_location varchar(55), artist_name varchar, song_id varchar(25), title varchar, duration decimal(10,5), year smallint);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id bigint IDENTITY PRIMARY KEY, start_time timestamp REFERENCES time (start_time) NOT NULL, user_id int REFERENCES users (user_id) NOT NULL, level char(4), song_id varchar REFERENCES song (song_id), artist_id varchar REFERENCES artist (artist_id), session_id int, location varchar, user_agent varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar(15), last_name varchar, gender char(1), level char(4));""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (song_id varchar PRIMARY KEY, title varchar NOT NULL, artist_id varchar REFERENCES artist (artist_id), year int, duration numeric NOT NULL);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, latitude decimal(8,6), longitude decimal(9,6));""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int, day smallint, week smallint, month smallint, year smallint, weekday smallint);""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
