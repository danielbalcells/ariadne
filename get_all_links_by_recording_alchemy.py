from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import mbdata.models as models
import pandas as pd
import psycopg2, uuid, csv
# to encode non ascii characters to csv without any decode errors we set "utf8" encoding as default encoding.
import sys;
reload(sys);
sys.setdefaultencoding("utf8")
import yaml


# Short script to query the DB
# With a lot of help from Albin Correya's scripts on GitHub

# Init connection to the DB
try:
    with open('conn_strings.yml', 'r') as stream:
        conn_strings = yaml.load(stream)

    #Connecting to the musicbrainz database using psycopg2
    conn = psycopg2.connect(dbname = conn_strings['dbname'],
                            user = conn_strings['user'], 
                            password = conn_strings['password'], 
                            host = conn_strings['host'])
    #Initiate the cursor
    cur = conn.cursor()

    #Connecting to the musicbrainz database using SQLAlchemy
    #Provide your databse login credentials here in the desired format. Refer to http://docs.sqlalchemy.org/en/latest/core/engines.html
    engine = create_engine(conn_strings['alchemy_string'], echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    print "Succesfully connected to the musicbrainz database..."

except:
    raise ValueError('Unable to connect to the database.., please check your database login credentials...')


# Query a recording
example_gid = '1a1f65ba-9de6-4364-adf5-4dc6f550348f'
results = session.query(models.Recording).filter(models.Recording.gid ==
        example_gid).all()
for recording in results:
    print recording.name
