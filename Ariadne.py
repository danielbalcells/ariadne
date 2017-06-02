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
    # MB Recording object containing the actual music
    mb.Recording rec
    # Inbound connection to this Knot
    Thread inThread
    # Knot that led to this Knot through inThread
    Knot prevKnot

    def __init__(self, r, iThread, pKnot):
        self.rec = r
        self.inThread = iThread
        self.prevKnot = pKnot
"""
A Thread is a link between two Knots. It is an abstraction of one or many MB
Links, as defined by its ThreadType.
"""
class Thread:
    # Knots connected by this Thread
    Knot fromKnot
    Knot toKnot
    # Type of connection between the Knots
    ThreadType thrType

    def __init__(self, fKnot, tKnot, tType):
        self.fromKnot = fKnot
        self.toKnot = tKnot
        self.thrType = tType
