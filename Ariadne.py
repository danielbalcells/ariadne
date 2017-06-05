from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import mbdata.models as mb
import pandas as pd
import yaml
import numpy as np

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
    
    # Determines whether a Thread can be started from a given Knot
    @staticmethod
    def isApplicable(db, fromKnot):
        raise NotImplementedError("Should have implemented this")
    
    # Returns the nResults "best" threads of a given type
    @staticmethod
    def rank(threads, nResults):
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
    
    # A ThreadBySameArtist can be started at any Knot
    @staticmethod
    def isApplicable(db, fromKnot):
        return True
    
    # Returning a random thread for now
    @staticmethod
    def rank(threads, nResults):
        best = np.random.choice(threads, nResults)
        return best

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
    
    # To get the next threads from a certain Knot, we get its artist (group).
    # We find all group members and the other groups they were part of.
    # For each member-group pair, we find some recordings.
    # For each member-group-recording combination, we make a new Thread.
    @staticmethod
    def getAllPossibleThreads(db, fromKnot, recsPerMemberGroupPair):
        fromRec = fromKnot.rec
        fromGroup = db.getArtistsByRecording(fromRec, 'FIRST')
        memberGroupPairs = db.getGroupsWithMembersInCommon(fromGroup)
        # Iterate member-group pairs and get some recordings to make Threads
        threads = []
        for mgPair in memberGroupPairs:
            recs = db.getRecordingsByArtist(mgPair['group'], 
                    select=recsPerMemberGroupPair)
            # Make a Thread for each member-group-recording combination
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
                       self.memberInCommon.name + ' in common with ' +\
                       self.fromGroup.name
        return renderString
    
    # This type of Thread only applies if the artist of the current Knot
    # is a Group
    @staticmethod
    def isApplicable(db, fromKnot):
        fromRec = fromKnot.rec
        fromArtist = db.getArtistsByRecording(fromRec, 'FIRST')
        return db.isGroup(fromArtist)
    
    # Returning a random thread for now
    @staticmethod
    def rank(threads, nResults):
        best = np.random.choice(threads, nResults)
        return best

"""
ThreadByGroupMemberSoloAct: A song by a group member's solo act
"""
class ThreadByGroupMemberSoloAct(Thread):
    # This Thread type additionally stores the name of the previous group, the
    # member in common, and the member's performing name (if it applies)
    def __init__(self, fKnot, tKnot, fGroup, member, mPerformsAs=None):
        super(ThreadByGroupMemberSoloAct, self).__init__(fKnot, tKnot)
        self.fromGroup = fGroup
        self.memberInCommon = member
        self.memberPerformsAs = mPerformsAs
    
    @staticmethod
    def getAllPossibleThreads(db, fromKnot, recsPerMemberActPair):
        fromRec = fromKnot.rec
        fromGroup = db.getArtistsByRecording(fromRec, 'FIRST')
        # Get solo Artists related to this Group's members
        memberActs = db.getMembersSoloActsByGroup(fromGroup)
        # Iterate acts and get some recordings to make Threads
        threads = []
        for memberAct in memberActs:
            recs = db.getRecordingsByArtist(memberAct['performsAs'], 
                    select=recsPerMemberActPair)
            for toRec in recs:
                toKnot = Knot(toRec, pKnot=fromKnot)
                newThread = ThreadByGroupMemberSoloAct(
                        fromKnot, toKnot,
                        fromGroup, 
                        memberAct['member'],
                        memberAct['performsAs'])
                threads.append(newThread)
        return threads
    
    def render(self):
        renderString = '"' + self.toKnot.rec.name + '" was written by ' +\
                       self.fromGroup.name + ' member ' +\
                       self.memberInCommon.name
        if self.memberInCommon.name != self.memberPerformsAs.name:
            renderString += ', who performs as ' + self.memberPerformsAs.name
        return renderString
    
    # This type of Thread only applies if the artist of the current Knot
    # is a Group
    @staticmethod
    def isApplicable(db, fromKnot):
        fromRec = fromKnot.rec
        fromArtist = db.getArtistsByRecording(fromRec, 'FIRST')
        return db.isGroup(fromArtist)
    
    # Returning a random thread for now
    @staticmethod
    def rank(threads, nResults):
        best = np.random.choice(threads, nResults)
        return best

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
    
    # Checks whether an Artist is a Group
    def isGroup(self, artist):
        return artist.type.name == 'Group'
    
    # Checks whether an Artist has any recordings credited to them
    def artistHasRecordings(self, artist):
        recordings = self.getRecordingsByArtist(artist, 'FIRST')
        if recordings:
            return True
        else:
            return False
    
    # Returns the Artist object(s) linked to a given Recording
    # Navigate from Artist table to Recording table through Credit tables,
    # get entries with matching Recording GID
    def getArtistsByRecording(self, rec, select):
        query = self.sess.query(mb.Artist)\
                         .join(mb.ArtistCreditName)\
                         .join(mb.ArtistCredit)\
                         .join(mb.Recording)\
                         .filter(mb.Recording.gid == rec.gid)
        return self.queryDB(query, select) 
    
    # Returns the Recording object(s) linked to a given Artist
    # Navigate from Recording table to Artist table through Credit tables,
    # get entries with matching Artist GID
    def getRecordingsByArtist(self, artist, select):
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
    
    # Returns the Artist(s) that a Person performs as
    def getArtistsPersonPerformsAs(self, person, select):
        query = self.sess.query(mb.LinkArtistArtist)\
                         .join(mb.Link)\
                         .join(mb.LinkType)\
                         .filter(mb.LinkArtistArtist.entity0 == person)\
                         .filter(mb.LinkType.name == 'is person')
        results = self.queryDB(query, select)
        artists = [link.entity1 for link in results]
        return artists
    
    # Returns the Artists that represent solo acts of members of a Group
    # These can be members playing directly under their real name, or
    # single-member acts with a name other than the member's
    # (e.g. Richard David James performs as Aphex Twin)
    def getMembersSoloActsByGroup(self, fromGroup):
        groupMembers = self.getMembersByGroup(fromGroup, 'ALL')
        acts = []
        # Iterate group members
        for member in groupMembers:
            # Get acts this member performs as
            memberPerformsAs = self.getArtistsPersonPerformsAs(member, 'ALL')
            for artist in memberPerformsAs:
                acts.append({'member': member, 'performsAs': artist})
            # Add the member if they have their own recordings
            if self.artistHasRecordings(member):
                acts.append({'member': member, 'performsAs': member})
        return acts

"""
An AriadneController executes the high-level logic behind the Ariadne workflow
"""
class AriadneController(object):
    # The AriadneController receives an AriadneDB object, an initial Recording,
    # and a list of the allowed Thread types. The constructor also initializes
    # a starting Knot from the initial Recording and lists for Threads and Knots
    # The Controller also stores the current Knot
    def __init__(self, db, sRec, aThreads):
        self.db = db
        self.startRec = sRec
        self.allowedThreads = aThreads
        self.startKnot = Knot(self.startRec)
        self.currentKnot = self.startKnot
        self.threads = []
        self.knots = [self.startKnot]
    
    # Calls the getAllPossibleThreads method of the applicable Thread
    # types
    def getAllPossibleThreads(self, applicableThreadTypes, select,
            fromKnot=None):
        if not fromKnot:
            fromKnot = self.currentKnot
        threads = []
        for ThreadType in applicableThreadTypes:
            thisTypeThreads = ThreadType.getAllPossibleThreads(
                                    self.db, fromKnot, select)
            threads.append({'type': ThreadType, 'threads': thisTypeThreads})
        return threads

    # Calls the isApplicable() method on the allowed Thread types
    def getApplicableThreadTypes(self, fromKnot=None):
        if not fromKnot:
            fromKnot = self.currentKnot
        return [tType for tType in self.allowedThreads  
                    if tType.isApplicable(self.db, fromKnot)]
    
    # Calls the rank() method on the provided Thread types, and returns only
    # the specified amount of Threads per Thread type.
    # Expects input threads in the form:
    # [{'type': ThreadType1, 'threads', [Thread1, Thread2]},
    #  {'type': ThreadType2, 'threads', [Thread3, Thread4]}]
    def rank(self, threads, nThreadsPerType):
        rankedThreads = []
        for typeThreadPair in threads:
            thisType = typeThreadPair['type']
            thisTypeThreads = typeThreadPair['threads']
            thisTypeRankedThreads = thisType.rank(thisTypeThreads,
                    nThreadsPerType)
            for thread in thisTypeRankedThreads:
                rankedThreads.append(thread)
        return rankedThreads
