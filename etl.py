import configparser
import psycopg2
#import boto3
from sql_queries import copy_table_queries, insert_table_queries, time_event_select, song_event_select, song_select
from time import time, strftime
import pandas as pd
import StringIteratorIO

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY             = config.get('AWS','KEY')
SECRET          = config.get('AWS','SECRET')

DWH_ENDPOINT    = config.get('DWH','DWH_ENDPOINT')
DWH_DB_NAME     = config.get('DWH','DWH_DB_NAME')
DWH_DB_USER     = config.get('DWH','DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH','DWH_DB_PASSWORD')
DWH_PORT        = config.get('DWH','DWH_PORT')

def load_staging_tables(cur, conn, schema):
    cur.execute("SET search_path TO " + schema + ";")
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        conn.close()

def insert_tables(cur, conn, schema):
    cur.execute("SET search_path TO " + schema + ";")
    for i, query in enumerate(insert_table_queries):
        if i == 0:
            print(query)
            artist_data = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
            cur.execute(query, artist_data)

        elif i == 1:
            print(query)
            song_data = ['song_id', 'title', 'artist_id', 'year', 'duration']
            cur.execute(query, song_data)

        elif i == 2:
            cur.execute("SET search_path TO stage;")
            print(time_event_select)
            cur.execute(time_event_select)
            df = pd.DataFrame(cur.fetchall())
            
            # Convert timestamp column values to datetime
            t = pd.to_datetime(df['ts'], unit='ms')

            # Break down datetime values into discrete time data records
            time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
            column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
            time_dict = {}
            for index, label in enumerate(column_labels):
                time_dict[label] = time_data[index]
            time_df = pd.DataFrame.from_dict(time_dict)

            # Execute time_table_insert query to insert time data records into the database
            for index, row in time_df.iterrows():
                #cur.execute(query, list(row))
                time_data = StringIteratorIO(list(row))
            
            cur.copy_from(time_data, 'dwh.time', sep=',', size=1024)

        elif i == 3:
            print(query)
            user_data = ['userId', 'firstName', 'lastName', 'gender', 'level']
            cur.execute(query, user_data)

        elif i == 4:
            print(song_event_select)
            cur.execute(song_event_select)
            df = pd.DataFrame(cur.fetchall())
        
            # insert songplay records
            for index, row in df.iterrows():   
                # get songid and artistid from song and artist tables
                cur.execute(song_select, (row.song, row.artist, row.length))
                results = cur.fetchone()

                if results:
                    songid, artistid = results
                else:
                    songid, artistid = None, None

                # insert songplay record
                songplay_data = StringIteratorIO(pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
                #cur.execute(query, songplay_data)
            
            cur.copy_from(songplay_data, 'dwh.songplay', sep=',', size=1024)

    conn.commit()
    conn.close()
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    #conn="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_PORT)
    #print(conn)

    '''s3 = boto3.resource('s3',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    '''
    load_staging_tables(cur, conn, "stage")
    #insert_tables(cur, conn, "dwh")

    conn.close()


if __name__ == "__main__":
    main()