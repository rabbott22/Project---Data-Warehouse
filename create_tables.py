import configparser
import psycopg2
from sql_queries import create_schema_queries, drop_schema_queries, create_staging_table_queries, create_table_queries, drop_table_queries
from time import strftime

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

DWH_ENDPOINT    = config.get('DWH','DWH_ENDPOINT')
DWH_DB_NAME     = config.get('DWH','DWH_DB_NAME')
DWH_DB_USER     = config.get('DWH','DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH','DWH_DB_PASSWORD')
DWH_PORT        = config.get('DWH','DWH_PORT')

def drop_tables(cur, conn, schema):
    query1 = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}';".format(schema)
    print(query1)
    cur.execute(query1)
    print(type(cur.fetchone()))
    print(cur.fetchone())
    if (cur.fetchone() is None):
        print("Schema " + schema + " does not exist.")

    elif cur.fetchone() == schema: 
        print("Fetchone result is: " + cur.fetchone()[0])
        cur.execute("SET search_path TO " + schema + ";")
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    else:
        print("Schema " + schema + " does not exist.")

def drop_schemas(cur, conn):
    for query in drop_schema_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def create_schemas(cur, conn):
    for query in create_schema_queries:
        try:
            print(query)
            cur.execute(query)
        except Exception as e:
            print(e)
        conn.commit()

def create_tables(cur, conn, schema, queryList):
    cur.execute("SET search_path TO " + schema + ";")
    for query in queryList:
        try:
            print(query)
            cur.execute(query)
        except Exception as e:
            print(e)
        conn.commit()
    
    '''Remove DOCSTRING comments to enable logging
    # start log 
    af = open("artist_record.log", "a")
    af.write(strftime("%Y-%m-%d %H:%M:%S") + " " + artist_table_insert + '\n')
    new_ad = [str(i) for i in artist_data]
    af.write(' '.join(new_ad) + '\n')
    af.close()
    # end log
    '''

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn, "stage")
    drop_tables(cur, conn, "dwh")
    drop_schemas(cur, conn)
    create_schemas(cur, conn)
    create_tables(cur, conn, "stage", create_staging_table_queries)
    create_tables(cur, conn, "dwh", create_table_queries)

    conn.close()

if __name__ == "__main__":
    main()