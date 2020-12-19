# Advanced-Database-Systems

Replicated Concurrency Control and recovery Project.

We implement a distributed database, complete with multiversion concurrency control, deadlock detection, replication, and failure recovery.


## Run Experiments
 - Running the code on a test case (say, test1.txt)
```python
python main.py --test test1.txt
```
- For debug mode,
```python
python main.py --debug True test1.txt
```
## Install dependencies
```bash
 apt-get install python python-dev python-pip gcc libsqlite3-dev libssl-dev libffi-dev
```
