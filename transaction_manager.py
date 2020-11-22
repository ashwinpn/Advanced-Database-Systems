#from collections import defaultdict
#from queue import Queue
from site import *
from config_params import *
from site_class import *
from functools import cmp_to_key




class Transaction:
  def __init__(self, transactionID, timestamp):
    self.transactionID = transactionID
    self.timestamp = timestamp
    # state = {Active, Aborted, Committed}
    self.state = "Active"
    # True for readOnly
    self.read_type = False
    self.accessedSites = []
    self.dataLocked = set()
    self.requestToHandle = None


class TransactionManager:

  # Collect the transactionID, transactions, timestamp
  transactions = {}
  sites = {}

  # transactionId -> {transactionIds}
  # ex. a is waited by {b, c, d}, and d is waited by b
  waitedBy = {}

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

    self.transactions[transaction.transactionID] = transaction
    print("TransactionStart: ", transaction.transactionID)


  def endTransaction(self, transaction, token):
  # token [whether to commit / abort] determined by the transaction status
    if not self.transactions[transaction.transactionID]:
      print("No such transaction ", transaction.transactionID)
      return



    if (token == "Abort"):
      transaction.state = "Aborted"
      self.transactions.pop(transaction.transactionID)

      print("Transaction Aborted: ", transaction.transactionID)
    else:
      #token = Commit
      for siteId, site in self.sites.items():
        for dataId in transaction.dataLocked:
          site.commit(transaction.transactionID, dataId)

      transaction.state = "Committed"
      self.transactions.pop(transaction.transactionID)
      print("Transaction Committed: ", transaction.transactionID)

    # Remove from waits-for graph
    if transaction.transactionID in self.waitedBy:
      for waitingTransactionID in self.waitedBy[transaction.transactionID]:
        waitingSet = self.waitsFor[waitingTransactionID]
        waitingSet.remove(transaction.transactionID)
        if (len(waitingSet) == 0):
          del self.waitsFor[waitingTransactionID]
      del self.waitedBy[transaction.transactionID]

      # Inform waiting transactions to try to continue handling their requests

    # Remove from site locks
    for siteId, site in self.sites.items():
      site.releaseLocks(transaction.transactionID, transaction.dataLocked)
    transaction.dataLocked = set()


  def read(self, transaction, data):
    pass
    # if not self.transactions[transaction.transactionID]:
    #   print("No such transaction")
    # else:
    #   for dmng in data_managers:
    #     # Check for locks
    #     if lock.lockStatus == True:
    #       # If Read Lock, Then
    #       # If Write Lock, Then
    #       if transaction.transactionID == lock.TransactionID:
    #       # Need to commit
    #         res = data.value
    #       # Else, create read lock
    #       # locks.createLock(READ,transaction.transactionID,data.dataID)
    #     transaction.accessedSites += [str(dmng.siteID)]
    #     print("Read for",dmng.siteID, transaction.transactionID, data.dataID, res)

  def write(self, transaction, dataId, newValue):
    if not self.transactions[transaction.transactionID]:
      print("No such transaction with id", transaction.transactionID)
      return

    print(transaction.transactionID, "starting to write to", dataId, "new value", newValue)
    # Need to check site status
    # If all required sites are Available,
    # all write locks can be accessed
    for siteId, site in self.sites.items():
      print("Checking to get Write lock for site", siteId)
      waitForTrans = site.checkLock("WRITE", transaction.transactionID, dataId)
      print("waitForTrans", waitForTrans)
      if (waitForTrans is None):
        continue

      if len(waitForTrans) == 0:
        print("+++ Got the Write lock for site", siteId)
        site.lock("WRITE", transaction.transactionID, dataId)
        transaction.dataLocked.add(dataId)
        site.update(transaction.transactionID, dataId, newValue)
        print(transaction.transactionID, "got the lock for", dataId, "and updated data to new value", newValue)

      else:
        self.waitForMethod(transaction, waitForTrans)
        transaction.requestToHandle = #assign new request
        print(transaction.transactionID, "did not get lock for", dataId, "will wait for tranasactions", waitForTrans)

  def waitForMethod(self, transaction, waitForTrans):
    if transaction.transactionID not in self.waitsFor:
      self.waitsFor[transaction.transactionID] = set()

    for tranId in waitForTrans:
      self.waitsFor[transaction.transactionID].add(tranId)
      if tranId not in self.waitedBy:
        self.waitedBy[tranId] = set()

      self.waitedBy[tranId].add(transaction.transactionID)


  def dump(self):
    for siteId in range(1, NUM_SITES+1):
        site = self.sites[siteId]
        flattened = site.flattenData()
        print("site " + str(siteId) + " " + flattened)


  def handleDeadlocks(self):
    print("Trying to find and handle deadlocks")
    deadlocks = [] # [(T#, T#)]
    visited = set()
    for tranId in self.waitsFor:
      if (tranId not in visited):
        currentlyVisiting = set()
        self.detectDeadlock(tranId, currentlyVisiting, visited, deadlocks)

    print("Result of finding deadlocks:", deadlocks)

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

    print("currTransId", currTransId, "destTransId", destTransId, "currPath", currPath, "visited", visited)

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
    print("res", res)
    return res


  def resolveDeadlocks(self, deadlocks):
    def transCompare(tranId1, tranId2):
      if not tranId1 in self.transactions or not tranId2 in self.transactions:
        raise Exception("Both tranIds should be in transactions list")
      return self.transactions[tranId1].timestamp < self.transactions[tranId2].timestamp


    transToAbort = []
    for pair in deadlocks:
      trans = self.getTransactionsInCycle(pair[1], pair[0])
      print("trans", trans)
      #trans.sort(key = transCompare)
      sorted(trans, key=cmp_to_key(transCompare))
      transToAbort.append(trans[0])
      print(trans)

    for tId in transToAbort:
      self.endTransaction(self.transactions[tId], "Abort")
    print("Resolved Deadlock by aborting following transactions", transToAbort)
