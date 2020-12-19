# Advanced-Database-Systems

<ins>Replicated Concurrency Control and recovery project.</ins>

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

## Troubleshooting
If you get this error:
```bash
E: Could not get lock /var/lib/dpkg/lock-frontend - open (11: Resource
temporarily unavailable)
E: Unable to acquire the dpkg frontend lock
(/var/lib/dpkg/lock-frontend),
is another process using it?
```

<ins>Run</ins>
```bash
sudo rm /var/lib/apt/lists/lock
sudo rm /var/cache/apt/archives/lock
sudo rm /var/lib/dpkg/lock*

sudo dpkg --configure -a

sudo apt update
```bash
