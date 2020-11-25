#!/bin/python
import argparse
from transaction_manager import *

time_tick = 0


class Database:

    TM = None

    def __init__(self, debug):
        self.TM = TransactionManager(debug)

    def listenAndHandleRequests(self, file_path):
        if file_path is None or len(file_path) == 0:
            # read from stdin
            pass
        else:
            with open(file_path, 'r') as f:
                for line in f:
                    request = self.getRequestFromLine(line)
                    if request is None:
                        continue
                    self.TM.handleRequest(request)

    def getRequestFromLine(self, line):
        global time_tick

        if (line == "\n"):
            time_tick += 1

        splitted = line.split(")")
        if (len(splitted) != 2):
            return None

        line = splitted[0]
        splitted = line.split('(')

        # Not valid line to parse
        if (len(splitted) != 2):
            return None

        requestType, params = splitted

        # Not valid request type
        if not all(c.isalpha() for c in requestType):
            return None

        splitted = params.split(",")
        param1 = param2 = param3 = None
        if len(splitted) == 1:
            if len(splitted[0]) > 0: param1 = splitted[0].strip()
        elif len(splitted) == 2:
            if len(splitted[0]) > 0: param1 = splitted[0].strip()
            if len(splitted[1]) > 0: param2 = int(splitted[1].strip()[1:])
        elif len(splitted) == 3:
            if len(splitted[0]) > 0: param1 = splitted[0].strip()
            if len(splitted[1]) > 0: param2 = int(splitted[1].strip()[1:])
            if len(splitted[2]) > 0: param3 = int(splitted[2].strip())
        else:
            return None

        return Request(requestType, param1, param2, param3)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', help='test input source', default='')
    parser.add_argument('--debug', help='test debug option', default='False')
    args = parser.parse_args()


    test_source = args.test
    debug = args.debug == "True"

    DB = Database(debug)
    DB.listenAndHandleRequests(test_source)






