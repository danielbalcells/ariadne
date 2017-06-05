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
    engine = create_engine(conn_strings['alchemy_string'], echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    print "Succesfully connected to the musicbrainz database..."

except:
    raise ValueError('Unable to connect to the database.., please check your database login credentials...')


# Query a recording
example_gid = '1a1f65ba-9de6-4364-adf5-4dc6f550348f'
cur.execute("select * from recording where gid='" + example_gid + "';")
recording = cur.fetchall()
recording_id = recording[0][0]
recording_id_str = str(recording_id)

# Iterate link tables
l_table_names = [   
                    'l_area_recording',
                    'l_artist_recording',
                    'l_event_recording',
                    'l_instrument_recording',
                    'l_label_recording',
                    'l_place_recording',
                    'l_recording_recording',
                    'l_recording_release',
                    'l_recording_release_group',
                    'l_recording_series',
                    'l_recording_url',
                    'l_recording_work'
                    ]

links = []

for l_table_name in l_table_names:
    # GENERATE QUERY STRING
    # Look for current recording in both entities if rec_rec link
    if l_table_name == "l_recording_recording":
        query_str = "select * from l_recording_recording " +\
                    "where entity0=" + recording_id_str + " " +\
                    "or entity1=" + recording_id_str +\
                    ";"
    # Look for current recording in left entity if rec_whatever link
    elif l_table_name.split('_')[1] == "recording":
        query_str = "select * from " + l_table_name + " " +\
                    "where entity0=" + recording_id_str +\
                    ";"
    elif l_table_name.split('_')[2] == "recording":
        query_str = "select * from " + l_table_name + " " +\
                    "where entity1=" + recording_id_str +\
                    ";"
    #print query_str

    # Query DB and append results to the links list
    cur.execute(query_str)
    query_results = cur.fetchall()
    if query_results:
        for item in query_results:
            links.append(item)

#print "\nFound links:"
#print links

# Iterate links and print information
for link in links:
    # Info from the l_*_* table:
    l_id = link[0]
    link_id = link[1]
    entity0 = link[2]
    entity1 = link[3]
    isEntity0 = entity0 == recording_id

    # Query link table with this link ID to get info
    query_str = "select * from link where id=" + str(link_id) + ";"
    cur.execute(query_str)
    link_data = cur.fetchall()[0]

    # Get link type from link_type table
    link_type_id = link_data[1]
    query_str = "select * from link_type where id=" + str(link_type_id) + ";"
    cur.execute(query_str)
    link_type_data = cur.fetchall()[0]
    entity0_type = link_type_data[4]
    entity1_type = link_type_data[5]
    link_phrase = link_type_data[10] # using short one for now
    reverse_link_phrase = link_type_data[9]

    # Get names of entity0 and entity1
    entity0_name_query = "select name from " + entity0_type + " " + \
                         "where id=" + str(entity0) + ";"
    cur.execute(entity0_name_query)
    entity0_name = cur.fetchall()[0][0]

    entity1_name_query = "select name from " + entity1_type + " " + \
                         "where id=" + str(entity1) + ";"
    cur.execute(entity1_name_query)
    entity1_name = cur.fetchall()[0][0]

    # If current recording is entity 0 use forward phrase
    print entity1_type
    if isEntity0:
        phrase = entity0_name + " " + link_phrase + " " + entity1_name
    else:
        phrase = entity1_name + " " + reverse_link_phrase + " " + entity0_name
    print phrase
