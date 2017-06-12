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
    def __init__(self, r, cArtists, iThread=None, pKnot=None):
        # MB Recording object containing the actual music
        self.rec = r
        # CreditedArtists object
        self.creditedArtists = cArtists
        # Inbound connection to this Knot
        self.inThread = iThread
        # Knot that led to this Knot through inThread
        self.prevKnot = pKnot

    # Returns a string describing the Knot
    def render(self):
        renderString = '"' + self.rec.name + '", by ' +\
                       self.creditedArtists.render()
        return renderString

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
        return renderString


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
                       ' was also written by ' +\
                       self.toKnot.creditedArtists.render()
        return renderString
    
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
            renderString = '"' + self.toKnot.rec.name + '" was written by ' + \
                       self.toGroup.name + ', that had group member ' +\
                       self.memberInCommon.name + ' in common with ' +\
                       self.fromGroup.name
        else:
            renderString = '"' + self.toKnot.rec.name + '" was written by ' + \
                       self.toKnot.creditedArtists.render() + '. ' +\
                       self.toGroup.name + ' had group member ' +\
                       self.memberInCommon.name + ' in common with ' +\
                       self.fromGroup.name
        return renderString
    
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
            renderString = '"' + self.toKnot.rec.name + '" was written by ' +\
                   self.fromGroup.name + ' member ' +\
                   self.memberInCommon.name
        else:
            renderString = '"' + self.toKnot.rec.name + '" was written by ' +\
                   self.toKnot.creditedArtists.artistList.render() + '. ' +\
                   self.fromGroup.name + ' had member ' +\
                   self.memberInCommon.name
        if self.memberInCommon.name != self.memberPerformsAs.name:
            renderString += ', who performs as ' + self.memberPerformsAs.name
        return renderString
    
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
        renderString = '"' + self.toKnot.rec.name + '" was written by ' +\
                self.toGroup.name + '. This band had member ' +\
            self.fromPerson.name 
        if self.memberPerformsAs:
            renderString += ', who performs as ' + self.memberPerformsAs.name
        return renderString

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
        self.startKnot = Knot(self.startRec, recCreditedArtists)
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
