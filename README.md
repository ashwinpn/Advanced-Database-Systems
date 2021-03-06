# Advanced-Database-Systems

<ins>Replicated Concurrency Control and recovery project.</ins>

We implement a distributed database, complete with multiversion concurrency control, deadlock detection, replication, and failure recovery.
Then, we use reprozip, docker, vagrant, and virtual machines to make our project reproducible even across different architectures for all time.


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
```

## Reprozip and Reproducibility
 - After installing dependencies, install reprozip.
 ```bash
 pip install -U reprozip
 ```
 - Install reprounzip.
```bash
pip install -U reprounzip[all]
```
- To collect traces, inside the project directory, run
```bash
reprozip trace python main.py --test <file_name>.txt

[When they ask you if you want to append to the trace, reply with ‘a’]
```
- Then pack
```bash
reprozip pack my_experiment.rpz
```
## Running the project
- With reprounzip
```bash
$ reprounzip directory setup my_experiment.rpz mydirectory
$ reprounzip directory run mydirectory
```
- With docker
```bash
$ reprounzip docker setup my_experiment.rpz mytemplate
$ reprounzip docker run mytemplate
```
![Terminal](https://github.com/ashwinpn/Advanced-Database-Systems/blob/main/resources/db1.JPG)

## Running with Vagrant
First, install the plugin reprounzip-vagrant.
```bash
$ pip install reprounzip-vagrant
```
Note that (i) you must install reprounzip first, and (ii) the plugin requires having Vagrant
installed. Then, use the prefix reprounzip vagrant to create and start a virtual machine
under the directory mytemplate:
```bash
$ reprounzip vagrant setup my_experiment.rpz mytemplate
```

To execute the experiment, simply run:
```bash
$ reprounzip vagrant run mytemplate
```

- We also ran it on Windows with vagrant and Oracle Virtual Box (Use <b>vagrant up</b> and
<b>vagrant ssh</b> to boot it up). You can find the vagrantfile and other files within the
Vagrant_DB folder.

![Terminal](https://github.com/ashwinpn/Advanced-Database-Systems/blob/main/resources/data1.JPG)
![Terminal](https://github.com/ashwinpn/Advanced-Database-Systems/blob/main/resources/data2.JPG)

## Theory
-  Use the available copies approach to replication using strict two phase locking (using read and write locks) at each site and validation at commit time.
-  Detect deadlocks using cycle detection and abort the youngest transaction in the cycle. The system keeps track of the transaction time of any transaction holding a lock.
-  Read-only transactions use multiversion read consistency.
-  The system consists of two parts: a single transaction manager that translates read and write requests on variables to read and write requests on copies using the available copy algorithm described in the notes [CSCI-GA-2434]. The transaction manager never fails. The other part is the site [or the data manager]. If a site fails and recovers, the
data manager would decide which in-flight transactions to commit.
- If a transaction T accesses an item (really accesses it, not just request
a lock) at a site and the site then fails, then T continues to execute
and then aborts only at its commit time (unless T is aborted earlier due to
deadlock).

## System Design

![](https://github.com/ashwinpn/Advanced-Database-Systems/blob/main/resources/uml.png)
