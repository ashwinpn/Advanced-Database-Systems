#from collections import defaultdict
#from queue import Queue
from site import *
from config_params import *




class Transaction:
  def __init__(self, transactionID, timestamp):
    self.transactionID = transactionID
    self.timestamp = timestamp
    # state = {Active, Aborted, Committed}
    self.state = "Active"
    # True for readOnly
    self.read_type = False
    self.accessedSites = []
    self.writeSites = []
    self.waitingRequest = None
    self.dataLocked = {}


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
      # token = Abort
      for site in self.sites:
        # Release present lock
        # Also release queued locks
        transaction.state = "Aborted"
        transactions.pop(transaction.transactionID)
        # Why did it abort?
        # Deadlock
        # Site Problem
      print("Transaction Aborted: ", transaction.transactionID)
    else:
      #token = Commit
      for site in self.sites:
        for dataId in dataLocked:
          site.commit(transaction.transactionID, dataId)

      transaction.state = "Committed"
      transactions.pop(transaction.transactionID)
      print("Transaction Committed: ", transaction.transactionID)

    if transaction.transactionID in waitedBy[transaction.transactionID]:
      for waitingTransactionId in waitedBy[transaction.transactionID]:
        waitingSet = waitsFor[waitingTransactionID]
        waitingSet.pop(transaction.transactionID)
        if (len(waitingSet) == 0):
          del waitsFor[waitingTransactionID]

      del waitedBy[transaction.transactionID]

    for site in self.sites:
      site.releaseLocks(transaction.dataLocked)
    transaction.dataLocked = {}


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
    for site in self.sites:
      waitForTrans = site.checkLock("WRITE", transaction.transactionID, dataId)
      if (waitForTrans is None):
        continue

      if len(waitForTrans) == 0:
        site.lock("WRITE", transaction.transactionID, dataId)
        transaction.dataLocked.add(dataId)
        site.update(transaction.transactionID, dataId, newValue)
        print(transaction.transactionID, "got the lock for", dataId, "and updated data to new value", newValue)

      else:
        self.waitForMethod(transaction, waitForTrans)
        print(transaction.transactionID, "did not get lock for", dataId, "will wait for tranasactions", waitForTrans)

    def waitForMethod(self, transaction, waitForTrans):
      if transaction.transactionID not in self.waitsFor:
        self.waitsFor[transaction.transactionID] = {}

      for tranId in waitForTrans:
        self.waitsFor[transaction.transactionID].add(tranId)
        if tranId not in self.waitedBy:
          self.waitedBy[tranId].add(transaction.transactionID)

      print("Write for", writeSites, transaction.transactionID, dataId, res)

  def dump(self):
    for siteId in range(1, NUM_SITES+1):
        site = self.sites[siteId]
        flattened = site.flattenData()
        print("site " + str(siteId) + " " + flattened)


  def handleDeadlocks(self):
    print("Trying to find and handle deadlocks")
    deadlocks = [] # [(T#, T#)]
    visited = {}
    for tranId in self.waitsFor:
      if (transId not in visited):
        currentlyVisiting = {}
        detectDeadlock(transId, currentlyVisiting, visited, deadlocks)

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

    currentlyVisiting.pop(currTransId)
    visited.add(currTransId)


  def transCompare(self, tranId1, tranId2):
    if not tranId1 in transactions or not tranId2 in transactions:
      raise Exception("Both tranIds should be in transactions list")

    return transactions[tranId1].timestamp < transactions[tranId2].timestamp

  def resolveDeadlocks(self, deadlocks):
    transToAbort = []
    for pair in deadlocks:
      trans = self.getTransactionsInCycle(pair[1], pair[0])
      trans.sort(key = self.transCompare)
      transToAbort.append(trans[0])

    for tId in transToAbort:
      self.endTransaction(self.transactions[tId], "Abort")
    print("Resolved Deadlock by aborting following transactions", transToAbort)
