from collections import defaultdict
from queue import Queue
from config_params import *

class Transaction:
  def __init__(self, transactionID, timestamp, state):
    self.transactionID = transactionID
    self.timestamp = timestamp
    # state = {Active, Aborted, Committed}
    self.state = state
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

  def __init__(self, sites):
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
      for site in sites:
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
      for site in sites:
        # Commit the data value
        # make data.status = PERMANENT
        # Release present lock
        # Also release queued locks
        pass

      transaction.state = "Committed"
      transactions.pop(transaction.transactionID)

      if transaction.transactionID in waitedBy[transaction.transactionID]:
        for waitingTransactionId in waitedBy[transaction.transactionID]:
          waitingSet = waitsFor[waitingTransactionID]
          waitingSet.pop(transaction.transactionID)

        del waitedBy[transaction.transactionID]

      print("Transaction Committed: ", transaction.transactionID)

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

  def write(self, transaction, data, value):
    if not self.transactions[transaction.transactionID]:
      print("No such transaction")
      return

    # Need to check site status
    # If all required sites are Available,
    # all write locks can be accessed
    for site in sites:
      waitForTrans = checkLock("WRITE", transaction, data)
      if len(waitForTrans) == 0:
        lock("WRITE", transaction, data)
        self.dataLocked.insert(data.dataId)
      else:
        self.waitFor(waitForTrans, transaction)


      # if data.dataId not in site.lockTable:
      #   site.lockTable[data.dataId] = Lock("WRITE", transaction)
      #   transaction.accessedSites.append(site.siteID)
      #   transaction.writeSites.append(site.siteID)
      # else:
      #   lock = site.lockTable[data.dataId]
      #   for t in lock.transactions:
      #     tId = t.transactionID
      #     if transaction.transactionID not in self.waitsFor:
      #       self.waitsFor[transaction.transactionID] = {}
      #     self.waitsFor[transaction.transactionID].add(tId)

      #     if tId not in self.waitedBy:
      #       self.waitedBy[tId] = {}
      #     self.waitedBy[tId].add(transaction.transactionID)

      print("Write for", writeSites, transaction.transactionID, data.dataId, res)

  def dump(self):
    for siteId in range(1, NUM_SITES+1):
        site = self.sites[siteId]
        flattened = site.flattenData()
        print("site " + str(siteId) + " " + flattened)

class DeadlockDetector:
  def detectDeadlock(self):
    pass
  def resolveDeadlock(self):
    pass




# class Lock:
#   def __init__(self, transaction, data, locktype):
#     # type = {Read, Write}
#     self.data = data
#     self.transaction = transaction
#     self.locktype = "unset"
#     self.lockStatus = False
#     if locktype=="Read":
#       # Many Transactions can share a readLock
#       self.locktype = locktype
#       self.transaction_list = [transaction.transactionID]
#       self.lockStatus = True

#     if locktype=="Write":
#       self.locktype = locktype
#       self.lockStatus = True

#   pLock = None
#   locks = []

#   def presentLock(lock):
#     pLock = lock

#   def changeLock(lock):
#     # Lock upgrade from read to write for a transaction
#     if lock.locktype != "Read":
#       print("Wrong lock type")
#     else:
#       sz = len(lock.transaction_list)
#       if not sz == 1:
#         print("Transactions are sharing the lock")
#       pLock = lock

#   def shareLock(lock):

