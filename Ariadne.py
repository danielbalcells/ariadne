from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import mbdata.models as mb
import pandas as pd
import yaml

"""
A Knot is one point in the music exploration. 
It wraps a Recording and how the exploration led to it.
It includes a MB Recording object, and Knot and Thread objects referring to the
previous recording and how it led to this one.
"""
class Knot(object):
    def __init__(self, r, iThread=None, pKnot=None):
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
class Thread(object):
    def __init__(self, fKnot, tKnot):
        # Knots connected by this Thread
        self.fromKnot = fKnot
        self.toKnot = tKnot

    # Queries the database to fin possible Knots implementing the specific
    # connection logic of each ThreadType
    @staticmethod
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
    @staticmethod
    def getAllPossibleThreads(db, fromKnot, limit=50):
        fromRec = fromKnot.rec
        fromArtist = db.getArtistsByRecording(fromRec, 'FIRST')
        toRecs = db.getRecordingsByArtist(fromArtist, limit)
        # Iterate possible recordings, create Threads and append to list
        threads = []
        for toRec in toRecs:
            toKnot = Knot(toRec, pKnot=fromKnot)
            newThread = ThreadBySameArtist(fromKnot, toKnot, fromArtist)
            toKnot.inThread = newThread
            threads.append(newThread)
        return threads

    def render(self):
        renderString = '"' + self.toKnot.rec.name + '"' +\
                       ' was also written by ' +\
                       self.artist.name
        return renderString

"""
ThreadByGroupWithMembersInCommon: a recording by a group with a member in common
with the previous one.
"""
class ThreadByGroupWithMembersInCommon(Thread):
    # This Thread type additionally stores the names of both groups, and the
    # name of the group member in common
    def __init__(self, fKnot, tKnot, fGroup, tGroup, member):
        super(ThreadByGroupWithMembersInCommon, self).__init__(fKnot, tKnot)
        self.fromGroup = fGroup
        self.toGroup = tGroup
        self.memberInCommon = member

    @staticmethod
    def getAllPossibleThreads(db, fromKnot, recsPerMemberGroupPair):
        fromRec = fromKnot.rec
        fromGroup = db.getArtistsByRecording(fromRec, 'FIRST')
        memberGroupPairs = db.getGroupsWithMembersInCommon(fromGroup)
        # Iterate member-group pairs and get some recordings to make threads
        threads = []
        for mgPair in memberGroupPairs:
            recs = db.getRecordingsByArtist(mgPair['group'],
                    select=recsPerMemberGroupPair)
            # Make a thread for each member-group-recording combination
            for toRec in recs:
                toKnot = Knot(toRec, pKnot=fromKnot)
                newThread = ThreadByGroupWithMembersInCommon(
                        fromKnot, toKnot, 
                        fromGroup, tGroup=mgPair['group'],
                        member=mgPair['member'])
                toKnot.inThread = newThread
                threads.append(newThread)
        return threads

    def render(self):
        renderString = '"' + self.toKnot.rec.name + '" was written by ' + \
                       self.toGroup.name + ', that had group member ' +\
                       self.memberInCommon.name + ' in common with  ' +\
                       self.fromGroup.name
        return renderString



"""
An AriadneDB wraps the connection to the MusicBrainz database and provides
useful functions to access it
"""
class AriadneDB(object):
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
            results = query.limit(select).all()
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
                         .filter(mb.Recording.gid == rec.gid)
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

    # Returns the Person Artist(s) linked to a given Group Artist
    def getMembersByGroup(self, group, select):
        query = self.sess.query(mb.LinkArtistArtist)\
                         .join(mb.Link)\
                         .join(mb.LinkType)\
                         .filter(mb.LinkArtistArtist.entity1 == group)\
                         .filter(mb.LinkType.name == 'member of band')
        results = self.queryDB(query, select)
        members = [link.entity0 for link in results]
        return members

    # Returns the Group Artist(s) linked to a given Person Artist
    def getGroupsByMember(self, member, select):
        query = self.sess.query(mb.LinkArtistArtist)\
                         .join(mb.Link)\
                         .join(mb.LinkType)\
                         .filter(mb.LinkArtistArtist.entity0 == member)\
                         .filter(mb.LinkType.name == 'member of band')
        results = self.queryDB(query, select)
        groups = [link.entity1 for link in results]
        return groups

    # Returns the Group Artist(s) with members in common with another Group
    def getGroupsWithMembersInCommon(self, fromGroup):
        groupMembers = self.getMembersByGroup(fromGroup, 'ALL')
        # Iterate group members to find other groups, store them in pairs
        memberGroupPairs = []
        for member in groupMembers:
            # Get groups in which this member played
            membersGroups = self.getGroupsByMember(member, 'ALL')
            # Keep them if they're not the same one we had before
            for group in membersGroups:
                if group.gid != fromGroup.gid:
                    memberGroupPairs.append({'member': member, 'group': group})
        return memberGroupPairs
