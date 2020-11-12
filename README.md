# CourseraCloudProject
Capstone Project by Coursera for Cloud Computing Specialisation.

> This repository consists of the code, scripts and output of the analysis of US Transportation Dataset (1988-2008) for aviation and predicting the best flights on a given day such that there is minimum travel delay.<br>
> For Step by Step procedure and project report check out the documentation folder!

In this project I answer a particular set of questions such as the best flight on a given day, the most popular airports, the most on-time airlines, etc. 
For the analysis and storing of data, I use various big data tools like hadoop,Spark and Cassandra.<br>

Hadoop is used for Storing(HDFS) as well as Processing(Mapreduce) data. I use spark scala script for ETL and pyspark API for interacting with results generated from the Mapreduce code.

![](header.png)

## Installation and Setup

Copy the Publicly available snapshot of the dataset on Amazon aws to your local region. Create a volume from this snapshot and attach this volume to your EC2 Instance.<br>
For a step by step guide of how to setup a hadoop cluster using amazon EC2 instances, check out my medium post : <br>
After you have attached the volume to your EC2 instance, ssh into your instance and mount your new volume attached to your file system using the following commands:

OS X & Linux:

```sh
$ lsblk                              #TO list all the blocks
$ sudo mkdir /data                   #Creating the directory for mounting
$ mount /dev/xvdf /data              #mounting /dev/xvdf on the /data folder created
```

## Data Extraction, Transformation and Loading 

Navigate through the newly mounted volume and look for the aviation folder which is out dataset of interest. We need to store the data into HDFS.
The data consists of csv files which are compressed and stored in zip files. We need to unzip the files and store only csv files into HDFS.

```sh
for FILE in `ls $DATA_FOLDER/airline_ontime/*/*.zip`; do
	for CSV_NAME in `unzip -l $FILE  | grep csv | tr -s ' ' | cut -d ' ' -f4`; do
		unzip -p $FILE $CSV_NAME | $HADOOP_HOME/bin/hdfs dfs -put - $HDFS_TARGET_FOLDER/$CSV_NAME
		echo "$CSV_NAME from $FILE_NAME ready in $HDFS_TARGET_FOLDER"
	done 
done
```
We use the above code to extract the data to our HDFS target directory. Click here for the complete script.

_For more examples and usage, please refer to the [Wiki][wiki]._

Once data is moved to HDFS, I clean the data using spark. The two main steps performed in the DATA CLEANING are as follows: <br>
1) The Data consists of inconsistent number of columns, so we restrict the number of columns by selecting the minimum number of columns in a file.<br>
2) There are some rows which have null values in ARRIVAL DELAY column (for cancelled flights), I drop these rows as they do not contribute to our current analysis requirement and might throw errors during data processing.

We use the following script to perform Data cleaning and loading the same under /Cleaned_data folder in HDFS.

## Development setup

Once we are done with ETL, we move on to the development part where we write hadoop jobs for processing data. I have setup my project in Eclipse IDE(no special reason just more familiarity with the IDE. You can use any IDE you prefer)
Since we need to create a JAR file of our compiled classes, use MAVEN for building your projects which I feel is the easiest way to clean and build your project(in just a matter of clicks! instead of long commands for compiling your code in shell) 
Use POM.xml to specify all the dependencies for your java classes. You can refer to my code in this folder.

## Release History

* 0.2.1
    * CHANGE: Update docs (module code remains unchanged)
* 0.2.0
    * CHANGE: Remove `setDefaultXYZ()`
    * ADD: Add `init()`
* 0.1.1
    * FIX: Crash when calling `baz()` (Thanks @GenerousContributorName!)
* 0.1.0
    * The first proper release
    * CHANGE: Rename `foo()` to `bar()`
* 0.0.1
    * Work in progress

## Meta

Your Name – [@YourTwitter](https://twitter.com/dbader_org) – YourEmail@example.com

Distributed under the XYZ license. See ``LICENSE`` for more information.

[https://github.com/yourname/github-link](https://github.com/dbader/)

## Contributing

1. Fork it (<https://github.com/yourname/yourproject/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

<!-- Markdown link & img dfn's -->
[npm-image]: https://img.shields.io/npm/v/datadog-metrics.svg?style=flat-square
[npm-url]: https://npmjs.org/package/datadog-metrics
[npm-downloads]: https://img.shields.io/npm/dm/datadog-metrics.svg?style=flat-square
[travis-image]: https://img.shields.io/travis/dbader/node-datadog-metrics/master.svg?style=flat-square
[travis-url]: https://travis-ci.org/dbader/node-datadog-metrics
[wiki]: https://github.com/yourname/yourproject/wiki
