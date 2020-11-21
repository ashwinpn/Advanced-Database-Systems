from collections import defaultdict
from queue import Queue

class Transaction:
  def __init__(self, transactionID, timestamp):
    self.transactionID = transactionID
    self.timestamp = timestamp
    # state = {Active, Aborted, Committed}
    self.state = "Active"
    # True for readOnly
    self.read_type = False
    self.accessedSites = []


class TransactionManager:
  # Collect the transactionID, transactions, timestamp
  transactions = {}

  def __init__(self, sites):
    self.sites = sites
    self.data_managers = []
    for site in sites:
      self.data_managers += [str(site)]

  def startTransaction(self, transaction):
    transactionID = transaction.transactionID
    self.transactions[transactionID] = transaction
    print("TransactionStart: ", transactionID)

  def endTransaction(self, transaction):
    # Abort
    transaction.state = "Aborted"
    self.transactions.pop(transaction.transactionID)
    # Why did it abort?
    # Deadlock
    # Site Problem
    print("Transaction Aborted: ", transaction.transactionID)

    #Commit
    transaction.state = "Committed"
    self.transactions.pop(transaction.transactionID)
    print("Transaction Committed: ", transaction.transactionID)

  def read(self, transaction, data):
    if not self.transactions[transaction.transactionID]:
      print("No such transaction")
    else:
      # data manager deals with data and we read
      transaction.accessedSites += [str()]

