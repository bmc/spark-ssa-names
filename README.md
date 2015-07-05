# Introduction

The programs in this repo are, essentially, [Apache Spark][] ETL jobs that
convert first name data from the United States Social Security Administration
into [Parquet][] files, to allow the data to be used more easily from other
Spark programs.

# The Programs

Currently, there are two programs.

**`com.ardentex.spark.SSNNamesToParquet`**

`SSNNamesToParquet` converts the SSN name data from
<http://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-national-level-data>
to a Parquet table. This data consists of first name, gender, and totals for
each year from 1880 to 2014.

 
**`com.ardentex.spark.SSNNamesByStateToParquet`**

`SSNNamesByStateToParquet` converts the name data from
<https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-data-by-state-and-district-of->
to a Parquet table. This data contains first name information by state and
consists of first name, gender, totals, and state data for 1910 through 2009. 

# Compiling the Code

Clone the repo, cd into the repo, and run `./activator` to bring up
[SBT][]. Then, run this SBT command to compile and package the code:

    package

# Running the Programs

First, you'll need a copy of Spark 1.4.0. Install it so that the Spark
`bin` directory is in your PATH.

Then, download the appropriate data set from _data.gov_ and use the
`scripts/run.sh` shell script to run each program.

NOTE: The `run.sh` script uses the Spark Scala shell, `spark-shell`, 
so it will fail if the Spark `bin` directory is not in your path.

## `SSNNamesToParquet`

For the first data set (the baby names from 1880 to 2014), you'll download
a file called `names.zip`. Unpack it into a directory. Let's assume you
unpacked it into `/tmp/names`. Then, within the
cloned repo, run this command:

    sh scripts/run.sh com.ardentex.spark.SSNNamesToParquet /tmp/names names.parquet
 
You'll get a `names.parquet` directory that represents the corresponding 
Parquet table.

Within the Spark shell or a Spark program, you can then create a Spark
DataFrame from this file as follows:

```scala
// spark-shell
val df = sqlContext.read.parquet("names.parquet")
```

```python
# pyspark
df = sqlContext.read.parquet("names.parquet")
```

## `SSNNamesByStateToParquet`

For the first data set (the baby names from 1880 to 2014), you'll download
a file called `names.zip`. Unpack it into a directory. Let's assume you
unpacked it into `/tmp/names-by-state`. Then, within the
cloned repo, run this command:

    sh scripts/run.sh com.ardentex.spark.SSNNamesByStateToParquet /tmp/names-by-state names-by-state.parquet
 
You'll get a `names-by-state.parquet` directory that represents the corresponding 
Parquet table.


* Compile this 
* Use the `scripts/run.sh` shell script in this repo to ru 


[Apache Spark]: http://spark.apache.org/
[Parquet]: http://parquet.apache.org/
[SBT]: http://scala-sbt.org
