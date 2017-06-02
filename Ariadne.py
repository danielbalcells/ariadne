from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import mbdata.models as mb
import pandas as pd
import yaml

"""
A Knot is an Ariadne abstraction of a MB recording.
It includes a MB recording object, and Knot and Thread objects referring to the
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
A Thread is a link between two Knots. It is an abstraction of one or many MB
Links, as defined by its ThreadType.
"""
class Thread:
    def __init__(self, fKnot, tKnot, tType):
        # Knots connected by this Thread
        self.fromKnot = fKnot
        self.toKnot = tKnot
        # Type of connection between the Knots
        self.thrType = tType

"""
A ThreadType determines a way of establishing a Thread between two Knots. As an
abstract class, it is up to the specific implementation of a ThreadType
subclass to determine how two Knots might be connected. ThreadTypes implement
the Knot connection logic at the DB level.
"""
class ThreadType:
    # Renders the thread data into output
    def render(fromKnot, toKnot):
        raise NotImplementedError("Should have implemented this")
    # Queries the database to fin possible Knots implementing the specific
    # connection logic of each ThreadType
    def getAllPossibleThreads(db, fromKnot):
        raise NotImplementedError("Should have implemented this")


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
