from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import mbdata.models as mb
import pandas as pd
import yaml
import numpy as np
import json

"""
A Knot is one point in the music exploration. 
It wraps a Recording and how the exploration led to it.
It includes a MB Recording object, and Knot and Thread objects referring to the
previous recording and how it led to this one.
"""
class Knot(object):
    def __init__(self, r, cArtists, iThread=None, pKnot=None, knotID=-1):
        # MB Recording object containing the actual music
        self.rec = r
        # CreditedArtists object
        self.creditedArtists = cArtists
        # Inbound connection to this Knot
        self.inThread = iThread
        # Knot that led to this Knot through inThread
        self.prevKnot = pKnot
        # ID if Knot has been used
        self.id = knotID

    # Returns a string describing the Knot
    def render(self):
        renderString = '"' + self.rec.name + '", by ' +\
                       self.creditedArtists.render()
        return renderString.encode('utf-8')

    # Serialize object data into struct to be parsed for JSON encoding
    def serialize(self):
        struct = {
                'type': 'Knot',
                'recName': self.rec.name,
                'recGID': self.rec.gid,
                'creditedArtists': self.creditedArtists.render(),
                'id': self.id
                }
        return struct

"""
A CreditedArtists object wraps one or many MB Artist objects to which a given 
Recording is credited
"""
class CreditedArtists(object):
    def __init__(self, artistList):
        if type(artistList) is not list:
            artistList = [artistList]
        self.artistList = artistList
    
    def render(self):
        renderString = ''
        nArtists = len(self.artistList)
        if nArtists == 1:
            renderString = self.artistList[0].name
        elif nArtists == 2:
            renderString = self.artistList[0].name + ' and ' +\
                    self.artistList[1].name
        else:
            renderString = self.artistList[0].name
            for i in range(1, nArtists-1):
                renderString += ', ' + self.artistList[i].name
            renderString += ' and ' + self.artistList[nArtists-1].name
        return renderString.encode('utf-8')


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
    
    # Serializes Thread data to be encoded as JSON
    def serialize(self):
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
    descText = 'A song by the same artist'
    # This Thread type additionally stores an Artist object connecting the
    # recordings
    def __init__(self, fKnot, tKnot, artist):
        super(ThreadBySameArtist, self).__init__(fKnot, tKnot)
        self.artist = artist
    
    # To find all possible threads of this type given a starting recording, we
    # iterate all credited artists and find some recordings by them
    @staticmethod
    def getAllPossibleThreads(db, fromKnot, recsPerCreditedArtist=50):
        fromRec = fromKnot.rec
        fromArtists = fromKnot.creditedArtists
        threads = []
        for thisArtist in fromArtists.artistList:
            thisArtistRecs = db.getRecordingsByArtist(thisArtist,
                    recsPerCreditedArtist)
            thisArtistThreads = ThreadBySameArtist.makeThreads(db, fromKnot,
                    thisArtistRecs, thisArtist)
            threads += thisArtistThreads
        return threads
    
    def render(self):
        renderString = '"' + self.toKnot.rec.name + '"' +\
                       ', also by ' +\
                       self.toKnot.creditedArtists.render()
        return renderString.encode('utf-8')
    
    def serialize(self):
        struct = {
                'type': 'ThreadBySameArtist',
                'fromKnot': self.fromKnot.serialize(),
                'toKnot': self.toKnot.serialize(),
                'id': self.id,
                'artist': self.artist.name
                }
        return struct

    # A ThreadBySameArtist can be started at any Knot
    @staticmethod
    def isApplicable(db, fromKnot):
        return True
    
    # Returning a random thread per unique artist for now
    @staticmethod
    def rank(threads, nResults):
        uniqueArtists = np.unique([t.artist for t in threads])
        best=[]
        for thisArtist in uniqueArtists:
            thisArtistThreads = [t for t in threads if t.artist is thisArtist]
            best += np.random.choice(thisArtistThreads, nResults).tolist()
        return best

    # Returns a list of Threads given a starting Knot, a list of ending
    # Recordings and the artist that they share.
    @staticmethod
    def makeThreads(db, fromKnot, toRecs, fromArtist):
        threads = []
        for toRec in toRecs:
            toCreditedArtistsList = db.getArtistsByRecording(toRec, 'ALL')
            toCreditedArtists = CreditedArtists(toCreditedArtistsList)
            toKnot = Knot(toRec, toCreditedArtists, pKnot=fromKnot)
            newThread = ThreadBySameArtist(fromKnot, toKnot, fromArtist)
            toKnot.inThread = newThread
            threads.append(newThread)
        return threads

"""
ThreadByGroupWithMembersInCommon: a recording by a group with a member in common
with the previous one.
"""
class ThreadByGroupWithMembersInCommon(Thread):
    descText = 'A song by a group with a member in common'
    # This Thread type additionally stores the names of both groups, and the
    # name of the group member in common
    def __init__(self, fKnot, tKnot, fGroup, tGroup, member):
        super(ThreadByGroupWithMembersInCommon, self).__init__(fKnot, tKnot)
        self.fromGroup = fGroup
        self.toGroup = tGroup
        self.memberInCommon = member
    
    # To get the next threads from a certain Knot, we iterate all credited
    # groups.
    # We find all group members and the other groups they were part of.
    # For each member-group pair, we find some recordings.
    # For each member-group-recording combination, we make a new Thread.
    @staticmethod
    def getAllPossibleThreads(db, fromKnot, recsPerMemberGroupPair=50):
        fromRec = fromKnot.rec
        fromArtists = fromKnot.creditedArtists
        fromGroups = [artist for artist in fromArtists.artistList
                if db.isGroup(artist)]
        threads = []
        for fromGroup in fromGroups:
            memberGroupPairs = db.getGroupsWithMembersInCommon(fromGroup)
            # Iterate member-group pairs and get some recordings to make Threads
            for mgPair in memberGroupPairs:
                toRecs = db.getRecordingsByArtist(mgPair['group'], 
                        select=recsPerMemberGroupPair)
                # Make a Thread for each member-group-recording combination
                thisPairThreads = ThreadByGroupWithMembersInCommon.makeThreads(
                        db, fromKnot, toRecs, fromGroup, mgPair['group'],
                        mgPair['member'])
                threads += thisPairThreads
        return threads
    
    def render(self):
        if len(self.toKnot.creditedArtists.artistList) == 1:
            renderString = '"' + self.toKnot.rec.name + '" by ' + \
                       self.toGroup.name + ', that had group member ' +\
                       self.memberInCommon.name + ' in common with ' +\
                       self.fromGroup.name
        else:
            renderString = '"' + self.toKnot.rec.name + '" by ' + \
                       self.toKnot.creditedArtists.render() + '. ' +\
                       self.toGroup.name + ' had group member ' +\
                       self.memberInCommon.name + ' in common with ' +\
                       self.fromGroup.name
        return renderString.encode('utf-8')
    
    def serialize(self):
        struct = {
                'type': 'ThreadByGroupWithMembersInCommon',
                'fromKnot': self.fromKnot.serialize(),
                'toKnot': self.toKnot.serialize(),
                'id': self.id,
                'fromGroup': self.fromGroup.name,
                'toGroup': self.toGroup.name,
                'memberInCommon': self.memberInCommon.name
                }
        return struct

    # This type of Thread only applies if one of the artists of the current Knot
    # is a Group
    @staticmethod
    def isApplicable(db, fromKnot):
        fromRec = fromKnot.rec
        fromArtists = fromKnot.creditedArtists
        for cArtist in fromArtists.artistList:
            if db.isGroup(cArtist):
                return True
        return False
    
    # Returning a random thread for now
    @staticmethod
    def rank(threads, nResults):
        best = np.random.choice(threads, nResults).tolist()
        return best

    # Returns a list of Threads given a starting Knot, a list of ending
    # Recordings and the extra information for each of them: from/to groups and
    # member in common
    @staticmethod
    def makeThreads(db, fromKnot, toRecs, fromGroup, toGroup, member):
        threads = []
        for toRec in toRecs:
            toCreditedArtistsList = db.getArtistsByRecording(toRec, 'ALL')
            toCreditedArtists = CreditedArtists(toCreditedArtistsList)
            toKnot = Knot(toRec, toCreditedArtists, pKnot=fromKnot)
            newThread = ThreadByGroupWithMembersInCommon(
                    fromKnot, toKnot, 
                    fromGroup, toGroup,
                    member)
            toKnot.inThread = newThread
            threads.append(newThread)
        return threads

"""
ThreadByGroupMemberSoloAct: A song by a group member's solo act
"""
class ThreadByGroupMemberSoloAct(Thread):
    descText = 'A song by a member of the same band playing solo'
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
        fromArtists = fromKnot.creditedArtists
        fromGroups = [artist for artist in fromArtists.artistList
                if db.isGroup(artist)]
        # Get solo Artists related to this Group's members
        threads = []
        for fromGroup in fromGroups:
            memberActs = db.getMembersSoloActsByGroup(fromGroup)
            # Iterate acts and get some recordings to make Threads
            for memberAct in memberActs:
                toRecs = db.getRecordingsByArtist(memberAct['performsAs'], 
                        select=recsPerMemberActPair)
                thisMemberActThreads = ThreadByGroupMemberSoloAct.makeThreads(
                        db, fromKnot, toRecs, fromGroup, 
                        memberAct['member'], memberAct['performsAs'])
                threads += thisMemberActThreads
        return threads
    
    def render(self):
        if len(self.toKnot.creditedArtists.artistList) == 1:
            renderString = '"' + self.toKnot.rec.name + '" by ' +\
                   self.fromGroup.name + ' member ' +\
                   self.memberInCommon.name
        else:
            renderString = '"' + self.toKnot.rec.name + '" by ' +\
                   self.toKnot.creditedArtists.render() + '. ' +\
                   self.fromGroup.name + ' had member ' +\
                   self.memberInCommon.name
        if self.memberInCommon.name != self.memberPerformsAs.name:
            renderString += ', who performs as ' + self.memberPerformsAs.name
        return renderString.encode('utf-8')
    
    def serialize(self):
        struct = {
                'type': 'ThreadByGroupMemberSoloAct',
                'fromKnot': self.fromKnot.serialize(),
                'toKnot': self.toKnot.serialize(),
                'id': self.id,
                'fromGroup': self.fromGroup.name,
                'memberInCommon': self.memberInCommon.name
                }
        if self.memberPerformsAs:
            struct['memberPerformsAs'] = self.memberPerformsAs.name
        else:
            struct['memberPerformsAs'] = ''
        return struct
        return struct

    # This type of Thread only applies if the artist of the current Knot
    # is a Group
    @staticmethod
    def isApplicable(db, fromKnot):
        fromRec = fromKnot.rec
        fromArtists = fromKnot.creditedArtists
        for cArtist in fromArtists.artistList:
            if db.isGroup(cArtist):
                return True
        return False
    
    # Returning a random thread for now
    @staticmethod
    def rank(threads, nResults):
        best = np.random.choice(threads, nResults).tolist()
        return best

    # Returns a list of Threads given a starting Knot, a list of Recordings and
    # their respective group, member in common and performing name (if any)
    @staticmethod
    def makeThreads(db, fromKnot, toRecs, fromGroup, member, performsAs):
        threads = []
        for toRec in toRecs:
            toCreditedArtistsList = db.getArtistsByRecording(toRec, 'ALL')
            toCreditedArtists = CreditedArtists(toCreditedArtistsList)
            toKnot = Knot(toRec, toCreditedArtists, pKnot=fromKnot)
            newThread = ThreadByGroupMemberSoloAct(
                    fromKnot, toKnot,
                    fromGroup, 
                    member,
                    performsAs)
            toKnot.inThread = newThread
            threads.append(newThread)
        return threads



"""
ThreadByGroupPersonIsMemberOf: A song by a band that a person played in
Reciprocal of ThreadByGroupMemberSoloAct
"""
class ThreadByGroupPersonIsMemberOf(Thread):
    descText = 'A song by a band that the same artist was a member of'
    # This Thread type additionally stores the name of the previous person, the
    # name of the band, and the person's performing name (if it applies)
    def __init__(self, fKnot, tKnot, fPerson, tGroup, mPerformsAs=None):
        super(ThreadByGroupPersonIsMemberOf, self).__init__(fKnot, tKnot)
        self.fromPerson = fPerson
        self.toGroup = tGroup
        self.memberPerformsAs = mPerformsAs
    
    # To get Threads of this type from a given Knot, we take the credited
    # artists that are Persons or single-Person acts. We get the Groups that
    # these Persons played in, and get some Recordings from each group. 
    # For each Person-Group-Recording set, we make a Thread.
    @staticmethod
    def getAllPossibleThreads(db, fromKnot, recsPerPersonGroupPair):
        fromRec = fromKnot.rec
        fromArtists = [a for a in fromKnot.creditedArtists.artistList \
                if db.isPerson(a) or db.isSinglePersonAct(a)]
        threads = []
        for fromArtist in fromArtists:
            toGroups = []
            # Sort out artist and person identities for the link
            if db.isSinglePersonAct(fromArtist):
                mPerformsAs = fromArtist
                fromPerson = db.getPersonBySinglePersonAct(fromArtist)
                toGroups += db.getGroupsByMember(mPerformsAs, 'ALL')
            else:
                mPerformsAs = None
                fromPerson = fromArtist
            # Get groups this person played in, get recordings and make threads
            toGroups += db.getGroupsByMember(fromPerson, 'ALL')
            for toGroup in toGroups:
                toRecs = db.getRecordingsByArtist(toGroup,
                        recsPerPersonGroupPair)
                thisGroupThreads = ThreadByGroupPersonIsMemberOf.makeThreads(
                        db, fromKnot, toRecs, fromPerson, toGroup, mPerformsAs)
                threads += thisGroupThreads
        return threads
    
    def render(self):
        renderString = '"' + self.toKnot.rec.name + '" by ' +\
                self.toGroup.name + '. This band had member ' +\
            self.fromPerson.name 
        if self.memberPerformsAs:
            renderString += ', who performs as ' + self.memberPerformsAs.name
        return renderString.encode('utf-8')

    def serialize(self):
        struct = {
                'type': 'ThreadByGroupPersonIsMemberOf',
                'fromKnot': self.fromKnot.serialize(),
                'toKnot': self.toKnot.serialize(),
                'id': self.id,
                'fromPerson': self.fromPerson.name,
                'toGroup': self.toGroup.name,
                }
        if self.memberPerformsAs:
            struct['memberPerformsAs'] = self.memberPerformsAs.name
        else:
            struct['memberPerformsAs'] = ''
        return struct

    # This type of Thread only applies if any of the artists of the current Knot
    # is a Person or a single-Person act
    @staticmethod
    def isApplicable(db, fromKnot):
        fromRec = fromKnot.rec
        fromArtists = fromKnot.creditedArtists
        for cArtist in fromArtists.artistList:
            if db.isPerson(cArtist) or db.isSinglePersonAct(cArtist):
                return True
        return False

    # Returning a random thread for now
    @staticmethod
    def rank(threads, nResults):
        best = np.random.choice(threads, nResults).tolist()
        return best

    # Returns a list of Threads given a starting Knot, a list of Recordings,
    # and the extra info for each Recording: the name of the Person that links
    # them, their performing name (if any), and the name of the Group where
    # they played.
    @staticmethod
    def makeThreads(db, fromKnot, toRecs, fromPerson, toGroup, mPerformsAs):
        threads = []
        for toRec in toRecs:
            toCreditedArtistList = db.getArtistsByRecording(toRec, 'ALL')
            toCreditedArtists = CreditedArtists(toCreditedArtistList)
            toKnot = Knot(toRec, toCreditedArtists, pKnot=fromKnot)
            newThread = ThreadByGroupPersonIsMemberOf(fromKnot, toKnot,
                    fromPerson, toGroup, mPerformsAs)
            toKnot.inThread = newThread
            threads.append(newThread)
        return threads

"""
ThreadByArtistWithFestivalInCommon: a song by an artist that played in the same
event as the previous one
"""
class ThreadByArtistWithFestivalInCommon(Thread):
    descText = 'A song by an artist that played in the same festival'
    # This Thread type additionally stores the two artists, and the event in
    # common
    def __init__(self, fKnot, tKnot, fArtist, tArtist, festival):
        super(ThreadByArtistWithFestivalInCommon, self).__init__(fKnot, tKnot)
        self.fromArtist = fArtist
        self.toArtist = tArtist
        self.festival = festival

    # To get all possible Threads, we:
    # Get all Festivals this Artist played in
    # For each Festival, we:
    #   Get all Artists that played in the same Festival
    #   For each Artist, we:
    #       Get some recordings by this Artist
    #       Make Threads
    # Note: we only take the Artists that played in the same minor Event 
    # (e.g. same day, same stage or the same festival) because they're arguably
    # closer than the other Artists on other stages or days of the same
    # festival. However, to avoid visual clutter we display the name of the
    # major Event (e.g. the name of the full festival)
    @staticmethod
    def getAllPossibleThreads(db, fromKnot, recsPerArtist):
        fromRec = fromKnot.rec
        fromArtists = fromKnot.creditedArtists
        threads = []
        for fromArtist in fromArtists.artistList:
            thisArtistFestivals = db.getEventsByArtist(fromArtist, 'ALL',
                ['Festival'])
            for festival in thisArtistFestivals:
                majorFestival = db.getLargestEventByPart(festival)
                thisFestivalArtists = db.getArtistsByEvent(festival, 'ALL')
                thisFestivalArtists = [a for a in thisFestivalArtists \
                                        if a is not fromArtist]
                for festivalArtist in thisFestivalArtists:
                    toRecs = db.getRecordingsByArtist(festivalArtist,
                            recsPerArtist)
                    thisArtistThreads = \
                        ThreadByArtistWithFestivalInCommon.makeThreads( 
                                db,
                                fromKnot, toRecs, 
                                fromArtist, festivalArtist, majorFestival)
                    threads += thisArtistThreads
        return threads

    def render(self):
        renderString = '"' + self.toKnot.rec.name + '" by '
        if len(self.toKnot.creditedArtists.artistList) > 1:
            renderString += self.toKnot.creditedArtists.render() + '. ' +\
                self.toArtist.name + ' '
        else:
            renderString += self.toArtist.name + ', who '
        renderString += 'played in festival ' + self.festival.name +\
            ' with ' + self.fromArtist.name
        return renderString.encode('utf-8')

    def serialize(self):
        struct = {
                'type': 'ThreadByArtistWithFestivalInCommon',
                'fromKnot': self.fromKnot.serialize(),
                'toKnot': self.toKnot.serialize(),
                'id': self.id,
                'fromArtist': self.fromArtist.name,
                'toArtist': self.toArtist.name,
                'festival': self.festival.name
                }
        return struct

    # This type of Thread is applicable for any Artist
    @staticmethod
    def isApplicable(db, fromKnot):
        return True

    # Returning a random thread for now
    @staticmethod
    def rank(threads, nResults):
        best = np.random.choice(threads, nResults).tolist()
        return best

    # Returns a list of Threads given a starting Knot, a list of Recordings and
    # the additional info: starting and ending Artists and the Festival Event
    # in which they both played.
    @staticmethod
    def makeThreads(db, fromKnot, toRecs, fArtist, tArtist, festival):
        threads = []
        for toRec in toRecs:
            toCreditedArtistList = db.getArtistsByRecording(toRec, 'ALL')
            toCreditedArtists = CreditedArtists(toCreditedArtistList)
            toKnot = Knot(toRec, toCreditedArtists, pKnot=fromKnot)
            thisThread = ThreadByArtistWithFestivalInCommon( 
                    fromKnot, toKnot, fArtist, tArtist, festival)
            threads.append(thisThread)
        return threads


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
    
    # Checks whether an Artist is a Person
    def isPerson(self, artist):
        return artist.type.name == 'Person'
    
    # Checkes whether an Artist is a single-Person act
    def isSinglePersonAct(self, artist):
        return bool(self.getPersonBySinglePersonAct(artist))

    # Checks whether an Artist has any recordings credited to them
    def artistHasRecordings(self, artist):
        recordings = self.getRecordingsByArtist(artist, 'FIRST')
        if recordings:
            return True
        else:
            return False
    
    # Returns the Recording object that has the given GID
    def getRecordingByGID(self, gid):
        query = self.sess.query(mb.Recording)\
                         .filter(mb.Recording.gid == gid)
        return self.queryDB(query, 'FIRST')

    # Returns the Artist object that has the given GID
    def getArtistByGID(self, gid):
        query = self.sess.query(mb.Artist)\
                         .filter(mb.Artist.gid == gid)
        return self.queryDB(query, 'FIRST')

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
        if results:
            artists = [link.entity1 for link in results]
        else:
            artists = []
        return artists
    
    # Returns the Person linked to a single-Person Artist
    def getPersonBySinglePersonAct(self, act):
        query = self.sess.query(mb.LinkArtistArtist)\
                         .join(mb.Link)\
                         .join(mb.LinkType)\
                         .filter(mb.LinkArtistArtist.entity1 == act)\
                         .filter(mb.LinkType.name == 'is person')
        results = self.queryDB(query, 'FIRST')
        if results:
            person = results.entity0
        else:
            person = None
        return person

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

    # Returns the Events where a certain Artist performed.
    # Optionally, restrict to a certain Event type
    def getEventsByArtist(self, fromArtist, select, eventTypeNames=[]):
        query = self.sess.query(mb.LinkArtistEvent)\
                         .filter(mb.LinkArtistEvent.entity0 == fromArtist)
        artistEventLinks = self.queryDB(query, select)
        events=[]
        for eventTypeName in eventTypeNames:
                events += [l.entity1 for l in artistEventLinks \
                            if l.entity1.type and \
                            l.entity1.type.name == eventTypeName]
        if events and not eventTypeNames:
            events = [l.entity1 for l in artistEventLinks]
        return events

    # Returns the Artists that performed at a certain Event.
    def getArtistsByEvent(self, fromEvent, select):
        query = self.sess.query(mb.LinkArtistEvent)\
                         .filter(mb.LinkArtistEvent.entity1 == fromEvent)
        artistEventLinks = self.queryDB(query, select)
        artists = [l.entity0 for l in artistEventLinks]
        return artists

    # Returns the highest order Event that an Event was part of
    # (e.g. Festival > Day > Stage, get Festival by Stage)
    def getLargestEventByPart(self, fromEvent):
        largerEvent = self.getEventByPart(fromEvent)
        haveLargestEvent = False
        candidateEvent = fromEvent
        while not haveLargestEvent:
            if not largerEvent:
                largestEvent = candidateEvent
                haveLargestEvent = True
            else:
                candidateEvent = largerEvent
                largerEvent = self.getEventByPart(largerEvent)
        return largestEvent

    # Returns the Event that a given Event is immediately part of
    # (e.g. Festival > Day > Stage, get Day by Stage or Festival by Day)
    def getEventByPart(self, fromEvent):
        link =  self.sess.query(mb.LinkEventEvent)\
                    .filter(mb.LinkEventEvent.entity1 == fromEvent)\
                    .first()
        if link:
            return link.entity0
        else:
            return None


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
        recCreditedArtistsList = db.getArtistsByRecording(sRec, 'ALL')
        recCreditedArtists = CreditedArtists(recCreditedArtistsList)
        self.allowedThreads = aThreads
        self.startKnot = Knot(self.startRec, recCreditedArtists, knotID=0)
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
            if thisTypeThreads:
                thisTypeRankedThreads = thisType.rank(thisTypeThreads,
                        nThreadsPerType)
                rankedThreads += thisTypeRankedThreads
        return rankedThreads

    # Sets the currentKnot to the Knot at the given index of the knots list
    def moveCurrentKnot(self, newKnotIndex):
        self.currentKnot = self.knots[newKnotIndex]
        return self.currentKnot

"""
AriadneClientCLI: a command-line interface to run the Ariadne workflow
"""
class AriadneClientCLI(object):
    def __init__(self, conn_string_path='conn_strings.yml',
            possibleThreadsPerThreadType=5,
            rankedThreadsPerThreadType=1):
        conn_string = self.loadConnString(conn_string_path)
        self.db = AriadneDB(conn_string, False)
        self.runAriadne = True
        self.possibleThreadsPerThreadType = possibleThreadsPerThreadType
        self.rankedThreadsPerThreadType = rankedThreadsPerThreadType
    
    # Main method of the client
    def run(self):
        # Get starting Recording from user
        startingRec = self.inputRecording(self.db)
        # Get allowed Thread types from user
        allowedThreadTypes = self.getAllowedThreadTypes()
        # Initialize AriadneController
        self.ctrl = AriadneController(self.db, startingRec, allowedThreadTypes)
        # Loop main logic
        while self.runAriadne:
            # Get next step: follow Thread, refresh Threads, or move to a
            # previous Knot
            self.doStep()
    
    # Run a step of the Ariadne workflow: get Threads, get user input, act
    def doStep(self):
        # Get best Threads at current Knot
        bestThreads = self.getBestThreads()
        # Get user choice on next step
        choice, nOptions = self.getStepChoice(bestThreads)
        # Act accordingly
        if choice == nOptions-1: # Quit Ariadne
            self.runAriadne = False
            return
        elif choice == nOptions-2: # Move to another Knot
            moveToInd = self.getKnotToMove()
            self.ctrl.moveCurrentKnot(moveToInd)
            return
        elif choice == nOptions-3: # Refresh list of Threads
            return
        else: # Follow Thread
            self.followThread(bestThreads[choice])
            return

    # Get user's choice on the next step
    def getStepChoice(self, bestThreads):
        # Make list of options
        options = [t.render() for t in bestThreads]
        options += ['Refresh possible next songs',
                    'Move to a previous song',
                    'Quit']
        # Get user input
        preListStr = 'Current song: ' + self.ctrl.currentKnot.render()
        postListStr = 'Your choice:'
        choice = self.getSingleChoice(options, preListStr, postListStr)
        return choice, len(options)

    # Get user's choice on what previously explored Knot to move to
    def getKnotToMove(self):
        # Make list of options
        options = [k.render() for k in self.ctrl.knots]
        # Get user's choice
        preListStr = 'Previously visited songs:'
        postListStr = 'Your choice:'
        choice = self.getSingleChoice(options, preListStr, postListStr)
        return choice
    
    # Updates the Controller to follow a given Thread
    def followThread(self, thread):
        self.ctrl.knots.append(thread.toKnot)
        self.ctrl.currentKnot = thread.toKnot

    # Loads the YAML file with the connection strings
    @staticmethod
    def loadConnString(conn_string_path):
        with open(conn_string_path, 'r') as stream:
            conn_strings = yaml.load(stream)
        return conn_strings['alchemy_string']
    
    # Asks the user for a Recording by GID
    def inputRecording(self, db):
        defaultRecID = '084a24a9-b289-4584-9fb5-1ca0f7500eb3'
        haveValidRecordingID = False
        while not haveValidRecordingID:
            recID = raw_input('Enter MBID of a Recording'+\
                    '(type default for Californication):\n')
            if recID == 'default':
                recID = defaultRecID
            inputRec = db.getRecordingByGID(recID)
            if inputRec:
                haveValidRecordingID = True
            else:
                print('Bad input!') 
        return inputRec
    
    # Asks the user for the Thread types they want
    def getAllowedThreadTypes(self):
       # Get text descriptions for each Thread type
       threadTypes = Thread.__subclasses__()
       threadTypeDescs = [tType.descText for tType in threadTypes]
       # Get user's choice from the list of Thread types
       preListText = 'Available Thread types:'
       postListText = 'Enter the numbers of the types you want to use:'
       allowedThreadTypesInds = self.getMultipleChoice(threadTypeDescs,
                                preListText,
                                postListText)
       allowedThreadTypes = [threadTypes[i] for i in allowedThreadTypesInds]
       return allowedThreadTypes

    # Gets the user's choices from a list of strings
    def getMultipleChoice(self, stringList, preListText, postListText):
        # Items are presented with numbers 1-N
        listInts = [i+1 for i in range(len(stringList))]
        haveChoice = False
        while not haveChoice:
            print '\n' + preListText
            for i in listInts:
                print str(i) + ') ' + stringList[i-1]
            choicesStr = raw_input('\n' + postListText + '\n')
            # If the input is correct (i.e. only numerical), get indices in
            # string list from the ints the user input
            if choicesStr.replace(' ','').isdigit():
                haveChoice = True
                inputChars = [c for c in choicesStr]
                # Only keep inds in the right range
                goodChars = [c for c in inputChars if int(c) in listInts]
                inds = [int(c)-1 for c in goodChars]
            else:
                print('Bad input!')
        return inds

    # Gets the user's choices from a list of strings
    def getSingleChoice(self, stringList, preListText, postListText):
        # Items are presented with numbers 1-N
        listInts = [i+1 for i in range(len(stringList))]
        haveChoice = False
        while not haveChoice:
            print '\n' + preListText
            for i in listInts:
                print str(i) + ') ' + stringList[i-1]
            choiceStr = raw_input('\n' + postListText + '\n')
            # If the input is correct (i.e. only numerical and in the right
            # range), get the matching index
            if choiceStr.isdigit() and int(choiceStr) in listInts:
                haveChoice = True
                ind = int(choiceStr)-1
            else:
                print('Bad input!')
        return ind

    # Gets the best Threads starting at a certain Knot
    def getBestThreads(self, fromKnot=None):
        if not fromKnot:
            fromKnot = self.ctrl.currentKnot
        # Get Thread types that apply for current Knot
        applicableThreadTypes = self.ctrl.getApplicableThreadTypes(fromKnot)
        # Get Threads available from current Knot
        possibleThreads = self.ctrl.getAllPossibleThreads(
                applicableThreadTypes,
                self.possibleThreadsPerThreadType,
                fromKnot)
        # Filter possible Threads to get the "best" per type (random atm)
        bestThreads = self.ctrl.rank(possibleThreads,
                self.rankedThreadsPerThreadType)
        return bestThreads

"""
AriadneBackend: implements the server-side logic, relying on client-side actions
to update the controller status
"""
class AriadneBackend(object):
    def __init__(self,
            conn_string_path='conn_strings.yml',
            possibleThreadsPerThreadType=1,
            rankedThreadsPerThreadType=1,
            allowedThreadTypes=Thread.__subclasses__()):
        # Load DB connection details
        conn_string = AriadneClientCLI.loadConnString(conn_string_path)
        # Init DB connection
        self.db = AriadneDB(conn_string, False)
        self.possibleThreadsPerThreadType = possibleThreadsPerThreadType
        self.rankedThreadsPerThreadType = rankedThreadsPerThreadType
        self.allowedThreadTypes = allowedThreadTypes
        self.haveStartingRec = False
        self.threadCounter = 0
        self.knotCounter = 0

    def inputRecording(self, recID):
        defaultRecID = '084a24a9-b289-4584-9fb5-1ca0f7500eb3'
        if recID == 'default':
            recID = defaultRecID
        inputRec = self.db.getRecordingByGID(recID)
        if not inputRec:
            raise Exception('Wrong MBID')
        return inputRec

    # Main method of the workflow
    def run(self, inputRecID):
        # Get starting Recording from user
        startingRec = self.inputRecording(inputRecID)
        # Initialize AriadneController
        self.ctrl = AriadneController(self.db, startingRec, self.allowedThreadTypes)
        self.knotCounter +=1
    
    # Gets the best Threads starting at a certain Knot and stores them as
    # an attribute
    def updateBestThreads(self, fromKnot=None):
        if not fromKnot:
            fromKnot = self.ctrl.currentKnot
        # Get Thread types that apply for current Knot
        applicableThreadTypes = self.ctrl.getApplicableThreadTypes(fromKnot)
        # Get Threads available from current Knot
        possibleThreads = self.ctrl.getAllPossibleThreads(
                applicableThreadTypes,
                self.possibleThreadsPerThreadType,
                fromKnot)
        # Filter possible Threads to get the "best" per type (random atm)
        bestThreads = self.ctrl.rank(possibleThreads,
                self.rankedThreadsPerThreadType)
        # Assign IDs to new best threads and store as attribute
        self.bestThreads = self.assignThreadIDs(bestThreads)
        
    # Adds a unique ID to every thread
    def assignThreadIDs(self, threads):
        # Iterate threads to assign a counter equal to self.threadCounter
        for t in threads:
            self.threadCounter += 1
            t.id = self.threadCounter
        return threads

    # Updates the Controller to follow a Thread given its ID
    def followThread(self, threadID):
        # Iterate current bestThreads to get Thread with given ID
        haveThread = False
        for t in self.bestThreads:
            if t.id == threadID:
                haveThread = True
                thread = t
                break
        if haveThread:
            thread.toKnot.id = self.knotCounter
            self.ctrl.threads.append(thread)
            self.ctrl.knots.append(thread.toKnot)
            self.ctrl.currentKnot = thread.toKnot
            self.knotCounter += 1
        else:
            raise Exception('Wrong Thread ID')

    # Updates the Controller to change the current Knot given its ID
    def moveCurrentKnot(self, knotID):
        # Iterate Knots to get the one with given ID
        haveKnot = False
        for k in self.ctrl.knots:
            if k.id == knotID:
                haveKnot = True
                self.ctrl.currentKnot = k
                break
        if not haveKnot:
            raise Exception('Bad Knot ID')

