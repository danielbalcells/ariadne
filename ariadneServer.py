#!/flask/bin/python
# Web server that runs the Ariadne workflow in a client-server environment

import Ariadne

import sys

from flask import Flask, render_template, request, redirect, Response
import random, json

# Useful constants
ARIADNE_IP = '0.0.0.0'
ARIADNE_PORT = '5010'
DEBUG_NO_HTML = True

# Declare web server app and AriadneBackend object
app = Flask(__name__)
backend = None

# Starting point
@app.route('/')
def init():
    global backend
    backend = Ariadne.AriadneBackend()
    
    if DEBUG_NO_HTML:
        return 'Instantiated AriadneBackend.'
    else:
        return 'To be implemented'

# Receives a recording UUID to be used as a starting point
@app.route('/input-recording', methods=['POST'])
def inputRecording():
    # Get request content
    try:
        data = request.get_json()
        recording_mbid = data['MBID']
        backend.run(recording_mbid)
        result = backend.ctrl.currentKnot.rec.name
        backend.haveStartingRec = True
    except Exception as e:
        backend.haveStartingRec = False
        result = e.message

    if DEBUG_NO_HTML:
        return 'Correctly set starting recording to ' + result
    else:
        return 'To be implemented'

# Returns the best Threads starting at the current Knot
@app.route('/get-best-threads', methods=['POST'])
def getBestThreads():
    try:
        data = request.get_json()
        bestThreads = backend.getBestThreads()
        serializedThreads = [t.serialize() for t in bestThreads]
        result = json.dumps(serializedThreads)
    except Exception as e:
        result = e.message

    return result

if __name__ == '__main__':
    app.run(ARIADNE_IP, ARIADNE_PORT)
