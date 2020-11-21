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


class TransactionManager:

  # Collect the transactionID, transactions, timestamp
  transactions = {}
  sites = {}

  def __init__(self, sites):
    self.sites = {}
    for siteId in range(1, NUM_SITES+1):
        self.sites[siteId] = Site(siteId)

  def startTransaction(self, transaction):
    self.transactions[transaction.transactionID] = transaction
    print("TransactionStart: ", transactionID)


  def endTransaction(self, transaction, token):
  # token [whether to commit / abort] determined by the transaction status
    if not self.transactions[transaction.transactionID]:
      print("No such transaction")

    # token = Abort
    for dmng in data_managers:
      # Release present lock
      # Also release queued locks
      transaction.state = "Aborted"
      transactions.pop(transaction.transactionID)
      # Why did it abort?
      # Deadlock
      # Site Problem
      print("Transaction Aborted: ", transaction.transactionID)

    #token = Commit
    for dmng in data_managers:
      # Commit the data value
      # make data.status = PERMANENT
      # Release present lock
      # Also release queued locks
      pass

    transaction.state = "Committed"
    transactions.pop(transaction.transactionID)
    print("Transaction Committed: ", transaction.transactionID)

  def read(self, transaction, data):
    if not transactions[transaction.transactionID]:
      print("No such transaction")
    else:
      for dmng in data_managers:
        # Check for locks
        if lock.lockStatus == True:
          # If Read Lock, Then
          # If Write Lock, Then
          if transaction.transactionID == lock.TransactionID:
          # Need to commit
            res = data.value
          # Else, create read lock
          # locks.createLock(READ,transaction.transactionID,data.dataID)
        transaction.accessedSites += [str(dmng.siteID)]
        print("Read for",dmng.siteID, transaction.transactionID, data.dataID, res)

  def write(self, transaction, data, value):
    if not transactions[transaction.transactionID]:
      print("No such transaction")
    else:
      # Need to check site status
      # If all required sites are Available,
      # all write locks can be accessed
      writeSites = []
      for dmng in data_managers:
        # Check for locks
        if lock.lockStatus == True:
          # If Read Lock, Then
          # If Write Lock, Then
          if transaction.transactionID == lock.TransactionID:
            # Write lock already exists for the transaction
            data.value = value
            res = data.value
            return
          else:
            print("Error, other transaction has acquired the write lock")

        # Else, create write lock
        # locks.createLock(WRITE,transaction.transactionID,data.dataID)
        # data.value = value
        # res = data.value
        transaction.accessedSites += [str(dmng.siteID)]
        writeSites += [str(dmng.siteID)]
        print("Write for", writeSites, transaction.transactionID, data.dataID, res)

  def dump(self):
      for siteId in range(1, NUM_SITES+1):
          site = self.sites[siteId]
          flattened = site.flattenData()
          print("site " + str(siteId) + " " + flattened)

class DeadlockDetector:
  def detectDeadlock(self):
  def resolveDeadlock():




class Lock:
  def __init__(self, transaction, data, locktype):
    # type = {Read, Write}
    self.data = data
    self.transaction = transaction
    self.locktype = "unset"
    self.lockStatus = False
    if locktype=="Read":
      # Many Transactions can share a readLock
      self.locktype = locktype
      self.transaction_list = [transaction.transactionID]
      self.lockStatus = True

    if locktype=="Write":
      self.locktype = locktype
      self.lockStatus = True

pLock = None
locks = []

def presentLock(lock):
  pLock = lock

def changeLock(lock):
  # Lock upgrade from read to write for a transaction
  if lock.locktype != "Read":
    print("Wrong lock type")
  else:
    sz = len(lock.transaction_list)
    if not sz == 1:
      print("Transactions are sharing the lock")
    pLock = lock

def shareLock(lock):

