from config_params import *
from site_class import *
from util import *
from functools import cmp_to_key

import main

class Transaction:
  def __init__(self, transactionID, timestamp, isReadOnly):
    self.transactionID = transactionID
    self.timestamp = timestamp
    # state = {Active, Aborted, Committed}
    self.state = "Active"
    # True for readOnly
    self.read_type = False
    self.lockedSites = set()
    self.requestToHandle = None
    self.waitedBy = []
    self.isReadOnly = isReadOnly

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

class TransactionManager:

  # Collect the transactionID, transactions, timestamp
  transactions = {}
  sites = {}

  # transactionId -> {transactionIds}
  # ex. b -> {a, d}, c -> {a}, d -> {a}
  waitsFor = {}

  def __init__(self):
    self.sites = {}
    for siteId in range(1, NUM_SITES+1):
        self.sites[siteId] = Site(siteId)

  def startTransaction(self, transaction):
    if transaction.transactionID in self.transactions:
      print("Transaction", transaction.transactionID, "has already been started!")
      return

    transaction.waitedBy = []

    self.transactions[transaction.transactionID] = transaction

    print("TransactionStart: ", transaction.transactionID)

  def endTransaction(self, transaction, token):
    print("ENDING TRANSACTION", transaction.transactionID)
    #print("self.transactions", self.transactions, "self.waitsFor", self.waitsFor)
    # token [whether to commit / abort] determined by the transaction status
    if not self.transactions[transaction.transactionID]:
      print("No such transaction ", transaction.transactionID)
      return

    allLockedSitesAreAvailable = True
    print("transaction.lockedSites", transaction.lockedSites)
    for siteId, dataId, time in transaction.lockedSites:
      site = self.sites[siteId]
      if site.state != "AVAILABLE" or time < site.failTimes[-1]:
        print("Reason for abortion for siteId", siteId, "site.state", site.state, "transaction.timestamp", transaction.timestamp, "site.failTimes[-1]", site.failTimes[-1])
        allLockedSitesAreAvailable = False
        break


    # Abort transaction if T can not commit to all the sites which T locked
    if allLockedSitesAreAvailable == False:
      token = "Abort"

    print("allLockedSitesAreAvailable", allLockedSitesAreAvailable, "token", token)


    if (token == "Abort"):
      transaction.state = "Aborted"
      self.transactions.pop(transaction.transactionID)

      print("!!!!!!!!!!!!!!!!!!!   Transaction Aborted: ", transaction.transactionID)
    else:
      #token = Commit
      for siteId, dataId, time in transaction.lockedSites:
        site = self.sites[siteId]
        site.commit(transaction.transactionID, dataId)

      transaction.state = "Committed"
      self.transactions.pop(transaction.transactionID)
      print(".................. Transaction Committed: ", transaction.transactionID)


    #Do cleanups
    transactionsNotWaitingForAnyone = set()
    # Remove from waits-for graph
    for waitingTransId, transIds in self.waitsFor.items():
      if transaction.transactionID in transIds:
        transIds.remove(transaction.transactionID)
        if (len(transIds) == 0):
          transactionsNotWaitingForAnyone.add(waitingTransId)

    for tId in transactionsNotWaitingForAnyone:
      del self.waitsFor[tId]

    if transaction.transactionID in self.waitsFor:
      del self.waitsFor[transaction.transactionID]


    for siteId, dataId, time in transaction.lockedSites:
      site = self.sites[siteId]
      site.releaseLocks(transaction.transactionID, [dataId])

    transaction.lockedSites = set()

    print("transaction.waitedBy", transaction.waitedBy)
    for waitingTransactionID in transaction.waitedBy:
      if waitingTransactionID in self.transactions:
        request = self.transactions[waitingTransactionID].requestToHandle
        self.handleRequest(request)

    transaction.waitedBy = []


    # Transaction just finished so it is not waiting for other transactions anymore
    for tId, trans in self.transactions.items():
      if tId == transaction.transactionID:
        continue

      if transaction.transactionID in trans.waitedBy:
        trans.waitedBy.remove(transaction.transactionID)


  def readOnly(self, transaction, dataId):
    if not self.transactions[transaction.transactionID]:
      print("No such transaction with id", transaction.transactionID)
      return
    sitesHoldingData = []
    for siteId, site in self.sites.items():
      if (site.state != "AVAILABLE"):
        continue
      if (site.checkDataExists(dataId)):
        sitesHoldingData.append(site)

    if (not isDataReplicated(dataId)) and len(sitesHoldingData) == 1:
      site = sitesHoldingData[0]
      d = site.getDataReadOnly(dataId, transaction.timestamp)
      print(d.toString())
      return
    elif (isDataReplicated(dataId) and len(sitesHoldingData) > 1):
      for site in sitesHoldingData:
        if site.checkReadOnly(dataId, transaction.timestamp):
          d = site.getDataReadOnly(dataId, transaction.timestamp)
          print(d.toString())
          return

    print(transaction.transactionID, "could not find any valid copy to READ_ONLY for data", dataId)

  def read(self, transaction, dataId):
    if not self.transactions[transaction.transactionID]:
      print("No such transaction with id", transaction.transactionID)
      return

    print("read", "transactionID", transaction.transactionID, "dataId", dataId)

    for siteId, site in self.sites.items():
      if (site.state != "AVAILABLE"):
        continue

      print("Read from site", siteId)

      if (site.checkRead(dataId) == True):
        print("Trying to get READ lock on dataId", dataId, "in siteId", siteId)
        # Try to get the lock
        waitForTrans = site.checkLock("READ", transaction.transactionID, dataId)
        print("waitForTrans", waitForTrans)
        # Data is not present in site, so move on - not a failure to lock
        if (waitForTrans is None):
          continue

        if len(waitForTrans) == 0:
          print("+++ Got READ lock for site", siteId)
          site.lock("READ", transaction.transactionID, dataId)
          #print(transaction.transactionID, "got the read lock for", dataId)
          d = site.getData(dataId)
          transaction.lockedSites.add((siteId, dataId, main.time_tick))
          print(transaction.transactionID,"R",d.toString(), "from siteId", siteId)
        else:
          print("Didnt get read lock")
          self.waitForMethod(transaction, waitForTrans)
          transaction.requestToHandle = Request("R", transaction.transactionID, dataId, None)
          print(transaction.transactionID, "did not get read lock for", dataId, "will wait for tranasactions", waitForTrans)
        break

  def write(self, transaction, dataId, newValue):
    if not self.transactions[transaction.transactionID]:
      print("No such transaction with id", transaction.transactionID)
      return

    transaction.requestToHandle = None

    print(transaction.transactionID, "starting to write to", dataId, "new value", newValue)
    # Need to check site status
    # If all required sites are Available,
    # all write locks can be accessed

    soFarLockedSites = []
    lockedAllAvailableSites = True;
    for siteId, site in self.sites.items():
      if (site.state != "AVAILABLE"):
        continue

      #print("Checking to get Write lock for site", siteId, "for dataId", dataId)
      waitForTrans = site.checkLock("WRITE", transaction.transactionID, dataId)
      #print("waitForTrans", waitForTrans)

      # Data is not present in site, so move on - not a failure to lock
      if (waitForTrans is None):
        continue

      if len(waitForTrans) == 0:
        #print("+++ Got the Write lock for site", siteId)
        site.lock("WRITE", transaction.transactionID, dataId)
        soFarLockedSites.append(siteId)
        #print(transaction.transactionID, "got the lock for", dataId, "and updated data to new value", newValue)
      else:
        self.waitForMethod(transaction, waitForTrans)
        transaction.requestToHandle = Request("W", transaction.transactionID, dataId, newValue)
        lockedAllAvailableSites = False
        print(transaction.transactionID, "did not get write lock for", dataId, "will wait for tranasactions", waitForTrans)

    # If we couldnt get all locks for available sites release others
    if lockedAllAvailableSites == False:
      print("Did not get locks for all available sites for T", transaction.transactionID, "for dataId", dataId)
      for siteId in soFarLockedSites:
        site = self.sites[siteId]
        site.releaseLocks(transaction.transactionID, [dataId])
    else:
      print("Got locks for all available sites for T", transaction.transactionID, "for dataId", dataId)
      for siteId in soFarLockedSites:
        transaction.lockedSites.add((siteId, dataId, main.time_tick))
        site = self.sites[siteId]
        site.update(transaction.transactionID, dataId, newValue)

  def waitForMethod(self, transaction, waitForTrans):
    if transaction.transactionID not in self.waitsFor:
      self.waitsFor[transaction.transactionID] = set()

    for tranId in waitForTrans:
      self.waitsFor[transaction.transactionID].add(tranId)
      self.transactions[tranId].waitedBy.append(transaction.transactionID)

  def dump(self):
    for siteId in range(1, NUM_SITES+1):
        site = self.sites[siteId]
        flattened = site.flattenData()
        print("site " + str(siteId) + " " + flattened)

  def handleDeadlocks(self):
    #print("Trying to find and handle deadlocks")
    deadlocks = [] # [(T#, T#)]
    visited = set()
    for tranId in self.waitsFor:
      if (tranId not in visited):
        currentlyVisiting = set()
        self.detectDeadlock(tranId, currentlyVisiting, visited, deadlocks)

    if (len(deadlocks) > 0):
      print("****************************************** Found deadlocks:", deadlocks)
      self.resolveDeadlocks(deadlocks)

  def detectDeadlock(self, currTransId, currentlyVisiting, visited, deadlocks):

    currentlyVisiting.add(currTransId)

    if (currTransId in self.waitsFor):
      for transId in self.waitsFor[currTransId]:
        if (transId in currentlyVisiting):
          deadlocks.append((currTransId, transId))
        elif (transId not in visited):
          self.detectDeadlock(transId, currentlyVisiting, visited, deadlocks)

    currentlyVisiting.remove(currTransId)
    visited.add(currTransId)

  def getTransactionsInCycleHelper(self, currTransId, destTransId, visited, currPath, res):
    if currTransId in visited:
      return


    visited.add(currTransId)
    currPath.append(currTransId)

    #print("currTransId", currTransId, "destTransId", destTransId, "currPath", currPath, "visited", visited)

    if currTransId == destTransId:
      for p in currPath:
        res.append(p)
      return

    if (currTransId in self.waitsFor):
      for transId in self.waitsFor[currTransId]:
        self.getTransactionsInCycleHelper(transId, destTransId, visited, currPath, res)

    currPath.pop()

  def getTransactionsInCycle(self, parentTransId, childTransId):
    path = []
    res = []
    visited = set()
    self.getTransactionsInCycleHelper(childTransId, parentTransId, visited, path, res)
    print("Transactions in cycle", res)
    return res

  def sortTransByAge(self, trans):
    def transCompare(tranId1, tranId2):
      if not tranId1 in self.transactions or not tranId2 in self.transactions:
        raise Exception("Both tranIds should be in transactions list")
      return self.transactions[tranId1].timestamp < self.transactions[tranId2].timestamp

    return sorted(trans, key=cmp_to_key(transCompare))

  def resolveDeadlocks(self, deadlocks):
    transToAbort = []
    for pair in deadlocks:
      trans = self.getTransactionsInCycle(pair[1], pair[0])
      print("trans", trans)
      #trans.sort(key = transCompare)
      trans = self.sortTransByAge(trans)
      transToAbort.append(trans[0])
      print("trans", trans)

    print("Resolved Deadlock by aborting following transactions", transToAbort)
    for tId in transToAbort:
      self.endTransaction(self.transactions[tId], "Abort")

  def handleRequest(self, request):
    # None has been passed as request
    if not request:
      return

    requestType, param1, param2, param3 = request.flatten()
    print("-------- Handling request", request.requestType, request.param1, request.param2, request.param3)

    self.handleDeadlocks()

    main.time_tick += 1
    print("main.time_tick", main.time_tick)
    if requestType == "begin":
      transaction = Transaction(param1, main.time_tick, False)
      self.startTransaction(transaction)
    elif requestType == "beginRO":
      transaction = Transaction(param1, main.time_tick, True)
      self.startTransaction(transaction)
    elif requestType == "W":
      transaction = self.transactions[param1]
      if (transaction.isReadOnly):
        print("INVALID WRITE REQUEST HAS BEEN PASSED TO READONLY TRANSACTION", transaction.transactionID)
        return
      self.write(transaction, param2, param3)
    elif requestType == "R":
      transaction = self.transactions[param1]
      if transaction.isReadOnly:
        self.readOnly(transaction, param2)
      else:
        self.read(transaction, param2)
    elif requestType == "end":
      transaction = self.transactions[param1]
      self.endTransaction(transaction, "Commit")
    elif requestType == "fail":
      siteId = int(param1)
      site = self.sites[siteId]
      site.fail()
    elif requestType == "recover":
      siteId = int(param1)
      site = self.sites[siteId]
      site.recover()
    elif requestType == "dump":
      self.dump()
    else:
        "Not Implemented PASSING"
