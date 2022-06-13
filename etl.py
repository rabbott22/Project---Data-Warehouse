import configparser
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
            print("Artist Table Load Start: " + str(datetime.datetime.now()))
            cur.execute(query)
            print("Artist Table Load End: " + str(datetime.datetime.now()))
            
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
            time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
            print(type(time_data))
            #tl = open("time_data.log", "w", encoding="utf-8")
            #tl.write(str(time_data))
            #tl.close()
            column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
            time_dict = {}
            for index, label in enumerate(column_labels):
                time_dict[label] = time_data[index].to_dict()
            #print(time_dict.keys())
            #print(type(time_dict['timestamp']))
            #print(time_dict['timestamp'])
            #td = time_dict['timestamp'].to_dict()
            #print(type(td))
            #print(td)
            #tl = open("time_dict.log", "w", encoding="utf-8")
            #tl.write(str(time_dict))
            #tl.close()

            insert_execute_values_iterator(cur, time_dict, 1000)
            
            #time_list = time_dict['timestamp'].tolist()
            #print(type(time_list))
            '''with open("time_dict.log", "w", encoding="utf-8") as tl:
                for time in td:
                    #tl.write(str(time) + "\n")
                    tl.write(time)
                    #print(time)
            '''
            '''for i in range(len(time_list)):
                #print(type(time_list[i]))
                #print(time_list[i])
                time_list[i] = time_list[i].strftime("%Y-%m-%d %H:%M:%S.%f")
                #time_dict[i] = value.strftime("%Y-%m-%d %H:%M:%S.%f")
            
            t_series = pd.Series(time_list)
            time_dict['timestamp'] = t_series
            #for item in time_dict.items():
            #   print(item)
            print(type(time_dict['timestamp']))
            print(time_dict['timestamp'])
            for key in time_dict.keys():
                print(key)
            print(type(time_dict['weekday']))
            print(time_dict['weekday'])
            '''
            #print(type(time_dict[0]['timestamp']))
            #time_df = pd.DataFrame.from_dict(time_dict, orient='index').drop_duplicates(subset=['timestamp'])
            #time_df = pd.DataFrame.from_dict(time_dict).drop_duplicates(subset=['timestamp'])
            #print(time_df.head())
            '''print("Time Table Load Start: " + str(datetime.datetime.now()))
            eng = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB_NAME))
            time_df.to_sql('dwh.time', eng, 'dwh', index=False, if_exists='append')
            print("Time Table Load End: " + str(datetime.datetime.now()))
            '''
            '''
            time_df = pd.DataFrame.from_dict(time_dict, orient='index')
            print(time_df.head())
            time_list = time_df.to_dict(orient='records')
            print(time_list[0][0])
            for ind, value in time_list[0].items():
                print(type(ind))
                print(ind)
                print(type(value))
                print(value)
                time_list[0][ind] = value.strftime("%Y-%m-%d %H:%M:%S.%f")
            '''
            '''
            #time_list = list(time_dict.items())
            #for ind, row in enumerate(time_list):
            #    time_list[ind]['timestamp'] = pd.to_pydatetime(time_list[ind]['timestamp'])
            #time_list = [list(row) for row in time_df.itertuples(index=False)]
            tll = len(time_list)
            print("Time List length is: " + str(tll))
            print(time_list)
            print(time_list[0][1])
            print(time_list[1][1])
            print(time_list[6][1])
            print(type(time_list))
            print(type(time_list[0]))
            print(type(time_list[1]))
            print(type(time_list[2]))
            print(type(time_list[3]))
            print(type(time_list[4]))                        
            print(type(time_list[5]))
            print(type(time_list[6]))            
            '''
            #print(cur.mogrify(query, time_dict))
            
            #print("Time Table Load Start: " + str(datetime.datetime.now()))
            #psycopg2.extras.execute_batch(cur, query, Iterator[time_dict[str, Any]], page_size = 1000)
            #cur.executemany(query, time_dict)
            #print("Time Table Load End: " + str(datetime.datetime.now()))
            
            '''
            #Remove DOCSTRING comments to enable logging
            # start log 
            tf = open("time_record.log", "a")
            for i, row in time_df.iterrows():
                cur.execute(time_table_insert, list(row))
                # continue log 
                tf.write(strftime("%Y-%m-%d %H:%M:%S") + " " + time_table_insert + '\n')
                new_td = [str(i) for i in list(row)]
                tf.write(' '.join(new_td) + '\n')
            tf.close()
            # end log

            print(time_df.head())
            #for index, item in enumerate(time_dict):
            #    while index < 2:
             #       print(item)

            # Execute time_table_insert query to insert time data records into the database
            for index, row in time_df.iterrows():
                cur.executemany  (query, list(row))
                print(type(row))
                print(row)
            
            
            q = "SET search_path TO {};"
            print("SCHEMA: " + schema)
            print(cur.mogrify(q.format(schema)))
            cur.execute(q.format(schema))
            #cur.execute("SET search_path TO " + schema + ";")
            print(type(time_data))
            #print(time_data)
            print(cur.mogrify(time_data, 'dwh.time', sep=',', size=1024))
            cur.copy_from(time_data, 'dwh.time', sep=',', size=1024)
            

        elif i == 3:
            print(query)
            #user_data = ['userId', 'firstName', 'lastName', 'gender', 'level']
            #print(cur.mogrify(query, user_data))
            for q in query:
                print(cur.mogrify(query))
                cur.execute(query)

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
                #songplay_data = StringIteratorIO(pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
                #cur.execute(query, songplay_data)
            
            cur.copy_from(songplay_data, 'dwh.songplay', sep=',', size=1024)
            '''

@profile
def insert_execute_values_iterator(
    #connection,
    cursor,
    times: Iterator[Dict[str, Any]],
    page_size: int = 100,
) -> None:
    #with cur as cursor:
    #    create_staging_table(cursor)
    psycopg2.extras.execute_values(cursor, """
    INSERT INTO dwh.dwh VALUES %s;
        """, ((
            beer['id'],
            beer['name'],
            beer['tagline'],
            parse_first_brewed(beer['first_brewed']),
            beer['description'],
            beer['image_url'],
            beer['abv'],
            beer['ibu'],
            beer['target_fg'],
            beer['target_og'],
            beer['ebc'],
            beer['srm'],
            beer['ph'],
            beer['attenuation_level'],
            beer['brewers_tips'],
            beer['contributed_by'],
            beer['volume']['value'],
        ) for beer in beers), page_size=page_size)

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