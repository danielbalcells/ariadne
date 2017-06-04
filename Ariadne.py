from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import mbdata.models as mb
import pandas as pd
import yaml

"""
A Knot is one point in the music exploration. 
It wraps a Recording and how the exploration led to it.
It includes a MB Rong object, and Knot and Thread objects referring to the
previous recording and how it led to this one.
"""
class Knot:
    def __init__(self, r, iThread, pKnot):
        # MB Recording object containing the actual music
        self.rec = r
        # Inbound connection to this Knot
        self.inThread = iThread
        # Knot that led to this Knot through inThread
        self.prevKnot = pKnot
"""
A Thread is a link between two Knots. It wraps one or many MB Links.
As an abstract class, it is up to the specific implementation of a Thread
subclass to determine how two Knots might be connected.
"""
class Thread:
    def __init__(self, fKnot, tKnot, tType):
        # Knots connected by this Thread
        self.fromKnot = fKnot
        self.toKnot = tKnot

    # Queries the database to fin possible Knots implementing the specific
    # connection logic of each ThreadType
    @classmethod
    def getAllPossibleThreads(db, fromKnot):
        raise NotImplementedError("Should have implemented this")

    # Renders the thread data into output
    def render(self):
        raise NotImplementedError("Should have implemented this")

"""
ThreadBySameArtist: two recordings by the same artist
"""
class ThreadBySameArtist(Thread):
    # This Thread type additionally stores an Artist object connecting the
    # recordings
    def __init__(self, fKnot, tKnot, artist):
        super(ThreadBySameArtist, self).__init__(fKnot, tKnot)
        self.artist = artist

    # To find all possible threads of this type given a starting recording, we
    # first find its artist and then get all recordings by that artist.
    def getAllPossibleThreads(db, fromKnot, limit=50):
        fromRec = fromKnot.rec
        fromArtist = db.getArtistsByRecording(fromRec, 'FIRST')
        toRecs = db.getRecordingsByArtist(fromArtist, 50)

    def render(self):

"""
An AriadneDB wraps the connection to the MusicBrainz database and provides
useful functions to access it
"""
class AriadneDB:
    def __init__(self, connString, echo):
        engine = create_engine(connString, echo=echo)
        Session = sessionmaker(bind=engine)
        self.sess = Session()

    # Runs a query on the DB and returns the desired amount of results
    def queryDB(self, query, select):
        if select == 'ALL':
            results = query.all()
        elif select == 'FIRST':
            results = query.first()
        elif type(select) == int:
            results = query.limit(select)
        else:
            raise ValueError('Bad select value')
        return results

    # Returns the Artist object(s) linked to a given Recording
    def getArtistsByRecording(self, rec, select):
        # Navigate from Artist table to Recording table through Credit tables,
        # get entries with matching Recording GID
        query = self.sess.query(mb.Artist)\
                         .join(mb.ArtistCreditName)\
                         .join(mb.ArtistCredit)\
                         .join(mb.Recording)\
                         .filter(mb.Recording.gid == rec.gid)\
                         .all()
        return self.queryDB(query, select)

    # Returns the Recording object(s) linked to a given Artist
    def getRecordingsByArtist(self, artist, select):
        # Navigate from Recording table to Artist table through Credit tables,
        # get entries with matching Artist GID
        query = self.sess.query(mb.Recording)\
                                .join(mb.ArtistCredit)\
                                .join(mb.ArtistCreditName)\
                                .join(mb.Artist)\
                                .filter(mb.Artist.gid == artist.gid)
        return self.queryDB(query, select)
