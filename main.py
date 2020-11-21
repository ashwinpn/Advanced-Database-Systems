#!/bin/python
import argparse
from transaction_manager import *
from data_manager import *


class Request:
    requestType = None
    param1 = None
    param2 = None
    param3 = None
    def __init__(self, requestType, param1, param2, param3):
        self.requestType = requestType
        self.param1 = param1
        self.param2 = param2
        self.param3 = param3

    def flatten(self):
        return self.requestType, self.param1, self.param2, self.param3


class Database:
    time_tick = 0

    TM = None
    DM = None

    def __init__(self):
        self.DM = DataManager()
        self.TM = TransactionManager(self.DM.sites)

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
                    self.handleRequest(request)

    def getRequestFromLine(self, line):
        #print(line)
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
            if len(splitted[0]) > 0: param1 = splitted[0]
        elif len(splitted) == 2:
            if len(splitted[0]) > 0: param1 = splitted[0]
            if len(splitted[1]) > 0: param2 = splitted[1]
        elif len(splitted) == 3:
            if len(splitted[0]) > 0: param1 = splitted[0]
            if len(splitted[1]) > 0: param2 = splitted[1]
            if len(splitted[2]) > 0: param3 = splitted[2]
        else:
            return None

        return Request(requestType, param1, param2, param3)



    def handleRequest(self, request):
        requestType, param1, param2, param3 = request.flatten()

        if requestType == "begin":
            print("-------- Handling request", request.requestType, request.param1, request.param2, request.param3)
            return

            # Check for Deadlock detection here
            self.time_tick += 1
            transaction = Transaction(param1, self.time_tick)
            self.TM.startTransaction(transaction)
        elif requestType == "W":
            print("-------- Handling request", request.requestType, request.param1, request.param2, request.param3)
            return

            transaction = self.TM.transactions[param1]
            if (transaction == None):
                raise Exception("Write attempt has been passed to a transaction which has not been started yet")
            # Write transaction
        elif requestType == "end":
            print("-------- Handling request", request.requestType, request.param1, request.param2, request.param3)
            return
            transaction = self.TM.transactions[param1]
            if (transaction == None):
                raise Exception("End command has been passed to a transaction which has not been started yet")
            #TM.endTransaction(transaction)
        elif requestType == "dump":
            print("-------- Handling request", request.requestType, request.param1, request.param2, request.param3)
            self.DM.dump()
        else:
            "Not Implemented PASSING"



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', help='test input source', default='')
    args = parser.parse_args()


    test_source = args.test
    DB = Database()
    DB.listenAndHandleRequests(test_source)






