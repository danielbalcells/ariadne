# demo_backend.py
# Runs a quick demo of the Ariadne backend
import Ariadne
import yaml
import mbdata.models as mb

def getUserThreadChoice(threads):
    haveChoice = False
    threadRenders = [thread.render() for thread in threads]
    threadInts = [i+1 for i in range(len(threads))]
    while not haveChoice:
        print 'Possible next songs:'
        for i in threadInts:
            print str(i) + ') ' + threadRenders[i-1]
        choice = raw_input('Your choice:\n')
        if choice == 'q':
            return choice
        elif choice.isdigit() and int(choice) in threadInts:
            haveChoice = True
            return threads[int(choice)-1]
        else:
            print('Bad input!')

# Init DB connection
with open('conn_strings.yml', 'r') as stream:
    conn_strings = yaml.load(stream)
db = Ariadne.AriadneDB(conn_strings['alchemy_string'], False)
print 'Successfully connected to MB database'

# Get starting Recording
defaultRecID = '084a24a9-b289-4584-9fb5-1ca0f7500eb3'
haveValidRecordingID = False
while not haveValidRecordingID:
    recID = raw_input('Enter recording MBID of the starting point '+\
            '(type default for Californication):\n')
    if recID == 'default':
        recID = defaultRecID
    
    startingRec = db.sess.query(mb.Recording)\
                    .filter(mb.Recording.gid==recID).first()
    if startingRec:
        haveValidRecordingID = True
    else:
        print('Bad input!')

# Get allowed Thread types
threadTypes = Ariadne.Thread.__subclasses__()
threadTypeDescs = [tType.descText for tType in threadTypes]
threadTypeInts = [i+1 for i in range(len(threadTypes))]
haveAllowedThreadTypes = False
while not haveAllowedThreadTypes:
    print 'Available Thread types:'
    for i in threadTypeInts:
        print str(i) + ') ' + threadTypeDescs[i-1]
    allowedTypesStr = raw_input('\nEnter the numbers of the types you want to use:\n')
    if allowedTypesStr.replace(' ','').isdigit():
        haveAllowedThreadTypes = True
        inputChars = [c for c in allowedTypesStr]
        goodChars = [c for c in inputChars if int(c) in threadTypeInts]
        inds = [int(c)-1 for c in goodChars]
        allowedThreadTypes = [threadTypes[i] for i in inds]
    else:
        print('Bad input!')

# Init AriadneController
ctrl = Ariadne.AriadneController(db, startingRec, allowedThreadTypes)
print 'Initialized AriadneController'

# Loop main Ariadne logic
exitAriadne = False
while not exitAriadne:
    # Get Thread types that apply for current Knot
    applicableThreadTypes = ctrl.getApplicableThreadTypes()
    # Print current song
    print '\nCurrent song: ' + ctrl.currentKnot.render()
    # Get available Threads from current Knot
    possibleThreads = ctrl.getAllPossibleThreads(applicableThreadTypes, 5)
    # Filter possible threads to get the "best" per type (random atm)
    bestThreads = ctrl.rank(possibleThreads, 1)
    # Get user input to choose a Thread
    nextThread = getUserThreadChoice(bestThreads)
    if nextThread == 'q':
        break
    # Update controller and loop
    ctrl.threads.append(nextThread)
    ctrl.knots.append(nextThread.toKnot)
    ctrl.currentKnot = nextThread.toKnot

