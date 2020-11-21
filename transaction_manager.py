from collections import defaultdict
from queue import Queue

# Collect the transactionID, transactions, timestamp
transactions = {}


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
  def __init__(self, sites):
    self.sites = sites
    self.data_managers = []
    for site in sites:
      self.data_managers += [str(site)]

  def startTransaction(self, transaction):
    self.transactions[transactionID] = transaction 
    print("TransactionStart: ", transactionID)

  def endTransaction(self, transaction):
    # Abort
    transaction.state = "Aborted"
    transactions.pop(transaction.transactionID)
    # Why did it abort? 
    # Deadlock
    # Site Problem
    print("Transaction Aborted: ", transaction.transactionID)

    #Commit
    transaction.state = "Committed"
    transactions.pop(transaction.transactionID)
    print("Transaction Committed: ", transaction.transactionID)

  def read(self, transaction, data):
    if not transactions[transaction.transactionID]:
      print("No such transaction")
    else:
      # data manager deals with data and we read
      transaction.accessedSites += [str()]
