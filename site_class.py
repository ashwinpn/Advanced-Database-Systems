from config_params import *
from util import *
from transaction_manager import *

import main

class Data:
    def __init__(self, dataId, value, oldCommitedCopies):
        self.dataId = dataId
        self.name = "x"+str(dataId)
        self.value = value
        self.commited = main.time_tick

    def __init__(self, dataId, value):
        self.dataId = dataId
        self.name = "x"+str(dataId)
        self.value = value

    def copy(self):
        return Data(self.dataId, self.value)

    def toString(self):
        return self.name+": "+str(self.value)

    def __str__(self):
        return self.toString()

class Lock:
    def __init__(self, lockType, transactionID, data):
        # lockType = {READ, WRITE, READ_ONLY}
        self.lockType = lockType
        self.transactions = {transactionID}
        self.dataInMemory = data

class Site:

    def __init__(self, siteId):
        self.failTimes = [-1]

        # enum states {AVAILABLE, FAILED}
        self.state = "AVAILABLE"

        # int siteId
        self.siteId = siteId

        # Hash map dataId -> Lock
        self.lockTable = {}

        # Hash map dataId -> Data
        self.data = {}

        # timestamp -> value
        self.oldCommitedCopies = {}

        for dataId in range(1, NUM_DATA+1):
            if (dataId % 2 != 0):
                # x1 -> site 2, x3 -> site 4, x11 -> site 2, x15 -> site 6, etc.
                if 1 + (dataId % NUM_SITES) == siteId:
                    self.data[dataId] = Data(dataId, 10 * dataId)
                    self.oldCommitedCopies[dataId] = [(main.time_tick, self.data[dataId])]
            else:
                self.data[dataId] = Data(dataId, 10 * dataId)
                self.oldCommitedCopies[dataId] = [(main.time_tick, self.data[dataId])]

    def flattenData(self):
        res = []
        for dataId in range(1, NUM_DATA+1):
            if dataId in self.data:
                d = self.data[dataId]
                res.append(d.name+": "+str(d.value))
        return ", ".join(res)

    # returns set of transactionIds must be waited for
    def checkLock(self, lockType, transactionID, dataId):
        if dataId not in self.data:
            return None

        if dataId not in self.lockTable:
            return set()

        presentLock = self.lockTable[dataId]

        if (lockType == "WRITE"):
            print(lockType, presentLock.transactions)
            trans = presentLock.transactions.copy()
            trans.discard(transactionID)
            return trans
        elif (lockType == "READ"):
            if (presentLock.lockType == "WRITE"):
                trans = presentLock.transactions.copy()
                trans.discard(transactionID)
                return trans
            elif (presentLock.lockType == "READ"):
                return set()


    def lock(self, lockType, transactionID, dataId):
        if dataId not in self.data:
            return

        # Transaction has already got same type of lock on this data
        if dataId in self.lockTable and lockType == self.lockTable[dataId].lockType and transactionID in self.lockTable[dataId].transactions:
            return

        # dataId has to be in self.data and we are adding copy of the data to lock table
        self.lockTable[dataId] = Lock(lockType, transactionID, self.data[dataId].copy())


    def update(self, transactionID, dataId, newValue):
        if dataId not in self.data:
            return

        if transactionID not in self.lockTable[dataId].transactions or self.lockTable[dataId].lockType != "WRITE":
            print(transactionID, "tries to update a data", dataId,"which it either does not have WRITE access or not any access at all")

        self.lockTable[dataId].dataInMemory.value = newValue

    def commit(self, transactionID, dataId):
        if dataId not in self.data:
            return

        if dataId not in self.lockTable:
            print(transactionID, "has passed commit command to data", dataId, "which is not locked")
            return

        lock = self.lockTable[dataId]
        if (lock.lockType == "WRITE"):
            if transactionID not in lock.transactions:
                print(transactionID, "tries to commit a data", dataId, "which it does not lock access at all")
            else:
                self.data[dataId] = lock.dataInMemory
                self.oldCommitedCopies[dataId].append((main.time_tick, self.data[dataId].copy()))


    def releaseLocks(self, transactionID, lockedDataIds):
        for dId in lockedDataIds:
            if dId not in self.data:
                continue
            if dId in self.lockTable:
                self.lockTable[dId].transactions.discard(transactionID)
                if len(self.lockTable[dId].transactions) == 0:
                    del self.lockTable[dId]

    def checkReadOnly(self, dataId, transTimestemp):
        if dataId not in self.data:
            None

        for oldCopy in self.oldCommitedCopies[dataId][::-1]:
            commitTime = oldCopy[0]
            if commitTime == -1:
                return False
            if commitTime < transTimestemp:
                return True

        return False

    def getDataReadOnly(self, dataId, transTimestemp):
        if dataId not in self.data:
            return None

        for oldCopy in self.oldCommitedCopies[dataId][::-1]:

            commitTime = oldCopy[0]
            if commitTime == -1:
                return None
            if commitTime < transTimestemp:
                return oldCopy[1]

        return None

    def checkDataExists(self, dataId):
        return dataId in self.data

    def checkReadAllowed(self, dataId, isDataReplicated):

        if dataId not in self.data:
            return None

        if isDataReplicated == False:
            return True

        return self.oldCommitedCopies[dataId][-1][0] != -1

    def getData(self, dataId):
        if dataId not in self.data:
            return
        return self.data[dataId]

    def fail(self):
        self.state = "FAILED"
        self.failTimes.append(main.time_tick)


    def recover(self):
        # make all data commited = False
        self.state = "AVAILABLE"
        for dataId, data in self.data.items():
            self.oldCommitedCopies[dataId].append((-1, None))

        self.lockTable = {}







