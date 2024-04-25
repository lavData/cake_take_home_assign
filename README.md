# Cake Home Assignment - ETL SFTP Servers

## Overview

Vu's assignment to build data pipeline to extract data from SFTP servers, transform the data and load it into destination SFTP servers.

## Deployment 

### 1. Gen SSH key
Because I deploy SFTP server on docker, so I need to generate SSH key 
avoid MITM warning when recreate container (and the host keys changes)
so must own the private key and public key
```bash
ssh-keygen -t ed25519 -f ssh_host_ed25519_key < /dev/null
ssh-keygen -t rsa -b 4096 -f ssh_host_rsa_key < /dev/null
```

### 2. Docker compose
```bash
docker-compose up
```

### 3. Run Sftp_sync dag

I have 2 SFTP servers, one for source and one for destination 
(actually they are the same server but different  user)
![img_1.png](img_1.png)

Steps:
- Go to Airflow UI at http://localhost:8080 with username and password is `airflow`
- Turn and trigger on the `Sftp_sync` dag
- Connect and put file to source SFTP server:
  -  Run `sftp -oPort=2222 source@localhost` with password is `src`
  - Cd to `upload` folder and put file 1.txt in my repo to this folder by `put 1.txt` command
- Check the destination SFTP  server (`sftp - oPort=2222 destination@localhost` go to dir `donwload`) to see the file is copied to this server 
(I schedule to run every 3 minutes so you need to wait a little bit)

**Note:** Connection source and dest SFTP have been created when deploy docker-compose

## Explain the pipeline

### 1. Design
![img_2.png](img_2.png)

This design is simple, I have 3 tasks: 
1. Detect all new files or modified files in the source
by using modify time of file and compare with the last modified file time in last run
, state can be stored in a database. If any file has modify time greater than the last modified time, it will be considered as a new file.
2. ETL data
3. Update the last modified time in the database as state, use this state in the next run

So it has the advantage that it just processes new files or modified files, so it can save time and resources.
But it requires more complex to handle the incremental modify time, manage state properly.  


### 3. The abstraction
Source or Destination not only SFTP server, it can be any storage file  like GCS, AWS S3, etc. So I have 2 abstract classes `Source` and `Destination` to define the interface for the source and destination. 
```python
class Source:
  def get_files(self, path):
    ...
    
  def get_mod_time(self, file_path):
    ...

  def get_pay_load(self, file_path):
    ...

  def get_new_files(self, path):
    ...

  def mark_file_processed(self, file_path):
    ...
```
It can be implemented for any storage file by override these methods, so I can easily change the source or destination in the future


### 4. Easily custom transform
Custom transform is needed in pipline, so I have defined the interface for the transform, so I can easily change the transform in the future

Trade-off: I assume that the transform just transform data inside the file, so can't  transform by  joining data from multiple files


### 5. Handle anomalies file size
I have two option to pipe data from source to destination:
1. Read all content of file and write to destination
   - Pros: simple, easy to implement
   - Cons: if the file is too large, it can cause memory error
2. Make a streaming for reading and writing file. I chunk the file into small pieces and pipe them to.
   - Pros: can handle large file
   - Cons: complex, need to handle the case when the file is not closed properly, IO pressure.
