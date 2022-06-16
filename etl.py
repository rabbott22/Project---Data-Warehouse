import configparser
from operator import concat
import psycopg2
import psycopg2.extras
#import boto3
from sql_queries import copy_table_queries, insert_table_queries, time_event_select, song_event_select, song_select
from time import time, strftime
import datetime
import pandas as pd
from sqlalchemy import create_engine
from typing import Iterator, Dict, Any
from time_mem_reporter import profile


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
        print(str(datetime.datetime.now()))
        print(query)
        cur.execute(query)
        conn.commit()
        print(str(datetime.datetime.now()))
    conn.commit()

def insert_tables(cur, conn, schema):
    cur.execute("SET search_path TO " + schema + ";")
    for i, query in enumerate(insert_table_queries):
        if i == 0:
            #print(query)
            #artist_data = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
            #q = cur.mogrify(query, artist_data)
            # '''Remove DOCSTRING comments to enable logging
            # start log 
            af = open("artist_record.log", "a")
            af.write(strftime("%Y-%m-%d %H:%M:%S") + " " + str(query) + '\n')
            #new_ad = [str(i) for i in artist_data]
            #af.write(' '.join(new_ad) + '\n')
            af.close()
            # end log
            # '''
            #cur.execute(query, artist_data)
            start = datetime.datetime.now()
            print("Artist Table Load Start: " + str(start))
            cur.execute(query)
            end = datetime.datetime.now()
            print("Artist Table Load End: " + str(end))
            elapsed = end - start
            print("Artist Table Load Time: " + str(elapsed))
            
        elif i == 1:
            print(query)
            song_data = ['song_id', 'title', 'artist_id', 'year', 'duration']
            print(cur.mogrify(query, song_data))
            print("Song Table Load Start: " + str(datetime.datetime.now()))
            cur.execute(query, song_data)
            print("Song Table Load End: " + str(datetime.datetime.now()))

        elif i == 2:
            cur.execute("SET search_path TO stage;")
            print(time_event_select)
            cur.execute(time_event_select)
            df = pd.DataFrame(cur.fetchall())
            print(df.head())
            print(type(df[0][0]))
            
            # Convert timestamp column values to datetime
            t = pd.to_datetime(df[0], unit='ms')
            print(type(t))
            print(df.loc[0])

            # Break down datetime values into discrete time data records
            #time_data = (t.to_string(index=False), t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
            #time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
            time_data = (t.to_dict(), t.dt.hour.to_dict(), t.dt.day.to_dict(), t.dt.week.to_dict(), t.dt.month.to_dict(), t.dt.year.to_dict(), t.dt.weekday.to_dict())
            print(type(time_data))
            #tl = open("time_data.log", "w", encoding="utf-8")
            #tl.write(str(time_data))
            #tl.close()
            column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
            time_dict = {}
            time_list = []
            j = 0
            tl = open("time_list.log", "w", encoding="utf-8")
            for key in time_data[j]:
            #    for index, label in enumerate(column_labels):
                tl.write("Key = " + str(key) + "\n")
                for index, label in enumerate(column_labels):
            #    time_dict[label] = time_data[index].to_dict()
                    time_dict[label] = time_data[index][key]
                    tl.write(label + " = " + str(time_data[index][key]) + "\n")
                    tl.write("index = " + str(index) + "\n")
                    #for key in time_dict[label]:
                tl.write(str(time_dict) + "\n")
                time_list.append(time_dict.copy())
                j += 1
                tl.write("i = " + str(j) + "\n")
                #tl.write(str(time_list) + "\n")
            tl.close()

            tlc = open("time_list_complete.log", "w", encoding="utf-8")
            tlc.write(str(time_list))
            tlc.close()

            insert_time_values_iterator(cur, time_list, 1000)
                   
        elif i == 3:
            print(query[0] + "\n" + query[1] + "\n" + query[2])
            #user_data = ['userId', 'firstName', 'lastName', 'gender', 'level']
            #print(cur.mogrify(query, user_data))
            for q in query:
                print(cur.mogrify(q))
                cur.execute(q)

        elif i == 4:
            print(song_event_select)
            cur.execute(song_event_select)
            #df = pd.DataFrame(cur.fetchall(), columns = ["artist", "length", "level", "location", "sessionId", "song", "ts", "userAgent", "userId"])
            df = pd.DataFrame(cur.fetchall())
            #song_select_vars = df.to_dict(orient="records")

            #songplay_df = pd.DataFrame(columns= ["start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent"])
            
            column_labels = ("start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent")
            sp_dict = {}
            sp_list = []
            #j = 0
            spl = open("songplay_list.log", "w", encoding="utf-8")
            # insert songplay records
            start = datetime.datetime.now()
            spl.write("songplay List Creation Start: " + str(start) + "\n")
            for index, row in df.iterrows():   
                # get songid and artistid from song and artist tables
                spl.write("\nIndex = " + str(index) + "\n")
                spl.write(str(datetime.datetime.now()) + " : " + "Start song_select query" + "\n")
                #tup = (row[5], row[0], row[1])
                mogstr = str(cur.mogrify(song_select, (row[5], row[0], row[1])))
                spl.write(mogstr)
                #print(cur.mogrify(song_select, (row[5], row[0], row[1])))
                cur.execute(song_select, (row[5], row[0], row[1]))
                spl.write("\n" + str(datetime.datetime.now()) + " : " + "End song_select query" + "\n")
                results = cur.fetchone()
                '''
                spl = open("songplay_list.log", "w", encoding="utf-8")
                # insert songplay records
                start = datetime.datetime.now()
                spl.write("songplay List Creation Start: " + str(start) + "\n")

                results = psycopg2.extras.execute_values(cur, song_select, ((
                    song_select_var['start_time', 'user_id', 'level'],
                ) for song_select_var in song_select_vars), page_size=1024, Fetch=True)
                

                #psycopg2.extras.execute_batch(cur, song_select, song_select_vars, page_size=1024)
                '''
                #spl.write("Results Count: " + str(len(results)) + " or " + str(cur.rowcount()) + "\n")
           
                if results:
                    songid, artistid = results
                else:
                    songid, artistid = None, None

                # insert songplay record
                #songplay_data = [(pd.to_datetime(row[6], unit='ms').to_dict, row[8].to_dict, row[2].to_dict, songid, artistid, row[4].to_dict, row[3].to_dict, row[7].to_dict)]
                sp_data = (pd.to_datetime(row[6], unit='ms'), row[8], row[2], songid, artistid, row[4], row[3], row[7])
                #sp_dict = {"start_time" : pd.to_datetime(song_select_vars["ts"], unit='ms'), "user_id" : song_select_vars["userId"], "level" : song_select_vars["level"], "song_id" : songid, "artist_id" : artistid, "session_id" : song_select_vars["sessionId"], "location" : song_select_vars["location"], "user_agent" : song_select_vars["userAgent"]}

                for index, label in enumerate(column_labels):
                    sp_dict[label] = sp_data[index]
                    spl.write(label + " = " + str(sp_data[index]) + "\n")
                    spl.write("index = " + str(index) + "\n")

                spl.write(str(sp_dict) + "\n")
                sp_list.append(sp_dict.copy())

            end = datetime.datetime.now()
            spl.write("songplay List Creation End: " + str(end) + "\n")
            elapsed = end - start
            spl.write("songplay List Creation Time: " + str(elapsed))
                
            spl.close()

            slc = open("songplay_list_complete.log", "w", encoding="utf-8")
            slc.write(str(sp_list))
            slc.close()
            
            #print(cur.mogrify(query, songplay_data))
            #cur.execute(query, songplay_data)
            
            #cur.copy_from(songplay_data, 'dwh.songplay', sep=',', size=1024)        

@profile
def insert_time_values_iterator(
    cursor,
    times: Iterator[Dict[str, Any]],
    page_size: int = 100,
) -> None:
    psycopg2.extras.execute_values(cursor, """
    INSERT INTO dwh.time VALUES %s;
        """, ((
            time['timestamp'],
            time['hour'],
            time['day'],
            time['week'],
            time['month'],
            time['year'],
            time['weekday'],
        ) for time in times), page_size=page_size)
'''
@profile
def insert_songplay_values_iterator(
    #connection,
    cursor,
    songplays: Iterator[Dict[str, Any]],
    page_size: int = 100,
) -> None:
    psycopg2.extras.execute_values(cursor, """
    INSERT INTO dwh.songplay VALUES %s;
        """, ((
            plays['start_time'],
            plays['user_id'],
            plays['level'],
            plays['song_id'],
            plays['artist_id'],
            plays['session_id'],
            plays['location'],
            plays['user_agent']
        ) for plays in songplays), page_size=page_size)
'''
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    #conn="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB_NAME)
    #print(conn)

    '''s3 = boto3.resource('s3',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    '''
    
    tabq = input("Load staging tables? (Y/N) ")
    if tabq in ("Y", "y"):
        load_staging_tables(cur, conn, "stage")
        tabq = input("Load data warehouse tables? (Y/N) ")
        if tabq in ("Y", "y"):
            insert_tables(cur, conn, "dwh")
            conn.commit()
            conn.close()
        elif tabq in ("N", "n"):
            conn.commit()
            conn.close()
        
    elif tabq in ("N", "n"):
        tabq = input("Load data warehouse tables? (Y/N) ")
        if tabq in ("Y", "y"):
            insert_tables(cur, conn, "dwh")
            conn.commit()
            conn.close()
        elif tabq in ("N", "n"):
            conn.commit()
            conn.close()
                    
if __name__ == "__main__":
    main()