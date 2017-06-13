#!/usr/bin/python
import Ariadne
import sys;
reload(sys);
sys.setdefaultencoding("utf8")

# Init CLI client
possibleThreadsPerThreadType = 5
rankedThreadsPerThreadType = 1
cli = Ariadne.AriadneClientCLI('conn_strings.yml',
        possibleThreadsPerThreadType,
        rankedThreadsPerThreadType)

# Run
cli.run()
