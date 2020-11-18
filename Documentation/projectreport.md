
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

* [Extraction script on GitHub](https://github.com/Ashwini130/CourseraCloudProject/tree/master/Scripts/moveDataToHadoop.sh)

```
$moveDataToHadoop /data /DATA
```
**✅ Step 1d. Extract the csv files to HDFS**:

- Clean the extracted files using Spark. The cleaning operations performed on the dataset were as follows:

1. Post Data Exploration and examining a subset of the data it was observed that there are few rows of data having null values in ArrDelay columns(for flights that were cancelled). These null values might interfere with out analysis so we drop these columns

```
df = df.na.drop(Array("ArrDelay"));
```
2. In the Description of Transportation DataSet, it is mentioned that for some years the data collected has more columns, (which are not significant to out analysis) so we restrict our data columns to about 56 columns only(as opposed to some csv files having 75 columns)

```
val sliceCols = df.columns.slice(0, 55)
df = df.select(sliceCols.map(name=>col(name)):_*)
```

* [ETL script on GitHub](https://github.com/Ashwini130/CourseraCloudProject/tree/master/Scripts/ETL_script.sh)

```
$spark-shell -i ETL_script.scala
```

(make sure you have your hadoop cluster already setup with spark installed over hadoop before running scala script. For quickly setting up a 4 node hadoop cluster, follow this link : )

## 2. Data Analysis using Hadoop and PySpark

Following diagram specifies the architecture used to integrate various big data tools used for analysis. We will explore them step by step when we answer the given questions.

![Architecture Image](images/architecture.PNG?raw=true)

* Group 1

** 1. Rank the top 10 most popular airports by numbers of flights to/from the airport.
