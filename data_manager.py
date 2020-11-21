from config_params import *

class Data:
    def __init__(self, dataId, value):
        self.dataId = dataId
        self.name = "x"+str(dataId)
        self.value = value

class Lock:
    def __init__(self, lockType, transaction):
        # lockType = {READ, WRITE, READ_ONLY}
        self.lockType = lockType
        self.transactions = [transaction]

class Site:
    def __init__(self, siteId):
        # enum states {Available, Not_Available, Recovering}
        self.state = "Available"

        # int siteId
        self.siteId = siteId

        # Hash map dataId -> Lock
        self.lockTable = {}

        # Hash map dataId -> Data
        self.data = {}
        for dataId in range(1, NUM_DATA+1):
            if (dataId % 2 != 0):
                # x1 -> site 2, x3 -> site 4, x11 -> site 2, x15 -> site 6, etc.
                if 1 + (dataId % NUM_SITES) == siteId:
                    self.data[dataId] = Data(dataId, 10 * dataId)
            else:
                self.data[dataId] = Data(dataId, 10 * dataId)

    def flattenData(self):
        res = []
        for dataId in range(1, NUM_DATA+1):
            if dataId in self.data:
                d = self.data[dataId]
                res.append(d.name+": "+str(d.value))
        return ", ".join(res)

    def fail(self, site):
        pass

    def recover(self, site):
        pass






