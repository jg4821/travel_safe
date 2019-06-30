import psycopg2
import psycopg2.extensions
import os
from collections import defaultdict
import pickle

''' This function queries the database and get all the unique countries in the db '''
def connectPostgres():
    try:
        conn = psycopg2.connect(database=os.environ['POSTGRES_DB'],user=os.environ['POSTGRES_USER'],
                                password=os.environ['POSTGRES_PASS'],host=os.environ['POSTGRES_URL'],port=5000)
    except Exception as er:
        print("Unable to connect to the database")
        print(str(er))

    cur = conn.cursor ()

    cur.execute ( "select distinct country from safety_score;")
    countries = cur.fetchall()

    res = []
    for c in sorted(countries):
        res.append((c[0],c[0]))

    cur.close ()
    conn.close ()

    with open('country.pkl', 'wb') as outfile:
        pickle.dump(res, outfile)
    return 

if __name__ == '__main__': 
    connectPostgres()
