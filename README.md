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
## Running
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
