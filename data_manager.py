
NUM_SITES = 10
NUM_DATA = 20

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
                if (dataId + 1) % NUM_SITES == siteId:
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

class DataManager:
    # dict of siteId -> Site
    sites = {}

    def __init__(self):
        self.sites = {}
        for siteId in range(1, NUM_SITES+1):
            self.sites[siteId] = Site(siteId)



    def fail(self, site):
        pass

    def recover(self, site):
        pass

    def dump(self):
        for siteId in range(1, NUM_SITES+1):
            site = self.sites[siteId]
            flattened = site.flattenData()
            print("site " + str(siteId) + " " + flattened)
