
## 🎓 Cloud Computing Capstone Project - Coursera 🎓

Follow along the following steps for a walkthrough of Project dev and outputs.

## Table of Contents

| Title  | Description
|---|---|
| **1. Data Extraction and Cleaning** | Mounting EBS snapshot, extracting and storing cleaned data in HDFS|
| **2. Data Analysis using Hadoop and PySpark** | Batch Processing using Hadoop and extracting results using PySpark API and storing information in Cassandra|
| **3. Solutions** | Solutions from the dataset for the questions like best flight on a given day, top 10 airports etc.|


## 1. Data Extraction and Cleaning


**✅ Step 1a. Create an AWS or AWS Educate Account** :
**✅ Step 1b. Mount EBS Snapshot**:

- Create a volume of the publicly available EBS Snapshot for Linux Machine in your region same as your EC2 Instance.
- Attach the volume to your instance and Mount it to your file system using the following commands:

```
$ lsblk                              //TO list all the blocks
$ sudo mkdir /data                   //Creating the directory for mounting
$ mount /dev/xvdf /data              //mounting /dev/xvdf on the /data folder created

```

**✅ Step 1c. Extract the csv files to HDFS**:

- The data stored in the EBS snapshot is in the form of csv files which are in a zipped folder. We use the following script to extract it to HDFS folder.



![Architecture Image](images/architecture.PNG?raw=true)

