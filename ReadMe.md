# PySpark & Spark
***

## What is Spark?
- Spark is a complete **in-memory framework**. Data gets loaded from, for instance HDFS, into the memory of workers. Data locality: processing data locally where it is stored is the most efficient thing to do. That's exactly what Spark is doing. You can and should run Spark workers directly on the data nodes of your Hadoop cluster.
- There is no longer a fixed map and reduce stage. Your code can be as complex as you want.
- Once in memory, the input data and the intermediate results stay in memory (until the job
finishes). They do not get written to a drive like with MapReduce.
- This makes Spark the optimal choice for doing complex analytics. It allows you for instance to do iterative processes. Modifying a dataset multiple times in order to create an output is totally easy.
- Streaming analytics capability is also what makes Spark so great. Spark has natively the option to schedule a job to run every X seconds or X milliseconds.
- As a result, Spark can deliver you results from streaming data in "real time".
- Apache Spark is written in Scala programming language. To support Python with Spark, Apache Spark community released a tool, PySpark.
***

## What is `MLlib` ?
- The best part of Spark is that it offers various built-in packages for machine learning, making it more versatile. These inbuilt machine learning packages are known as `ML-lib` in Apache Spark.
- MLlib is a very similar API to the scikit learn API. We don’t need to learn it separately.
- MLlib offers various types of Machine learning rebuild models.
- Spark’s `MLlib` supports computer vision as well as Natural Language Processing.
- It can be implemented on Realtime Data as well as distributed data systems.
***

## Understand why "Hadoop or Spark" is the totally wrong question!
- Compared to Hadoop, Spark is "just" an analytics framework. It has no storage capability. Although it has a standalone resource management, you usually don't use that feature.
- So, if Hadoop and Spark are not the same things, can they work together? As Storage you use HDFS. Analytics is done with Apache Spark and YARN is taking care of the resource management.
- It just would not make sense to have two resource managers managing the same server's resources. Sooner or later they will get in each others way. That's why the Spark standalone resource manager is seldom used. So, the question is not Spark or Hadoop. The question has to be: Should you use Spark or MapReduce alongside Hadoop's HDFS and YARN.
***

## When to use MapReduce and Apache Spark
- If you are doing simple batch jobs like counting values or doing calculating averages: Go with MapReduce.
- If you need more complex analytics like machine learning or fast stream processing: Go with Apache Spark.
***

## Installation
- Install [java](https://www.oracle.com/java/technologies/downloads/#java8)
- Install pyspark from Anaconda: `conda install -c conda-forge pyspark` alternatively use pip: `pip install pyspark`
- Download the latest version of Apache Spark from this [link](https://spark.apache.org/downloads.html), unzip it and place the folder in you home directory and change the folder name to just spark. 
- Define these environment variables, On Unix/Mac, this can be done in `.bashrc` or `.bash_profile.`
```
export SPARK_HOME=~/spark
# Tell spark which version of python you want to use
export PYSPARK_PYTHON=~/anaconda3/bin/python
```
- Verify installation:
```
cd spark
# launching pyspark
./bin/pyspark

# or to launch the scala console
./bin/spark-shell
```
- If you are using jupyter notebook:
    - Install java
    - Then `pip install pyspark`
***


## References
- [How to install PySpark locally](https://github.com/ethen8181/machine-learning/blob/master/big_data/spark_installation.md)
- [What is Spark](https://github.com/mikulskibartosz/Cookbook/blob/master/AdvancedSkills.md#data-science-platform)
- [Introduction to spark and its datasets](https://www.analyticsvidhya.com/blog/2022/08/introduction-to-on-apache-spark-and-its-datasets/)
- [YouTube #1](https://www.youtube.com/watch?v=WyZmM6K7ubc)
- [YouTube #2](https://www.youtube.com/watch?v=7I4YZwaJgPs)
- [YouTube #3](https://www.youtube.com/watch?v=pOMXkbc06m4)
- [YouTube #4](https://www.youtube.com/watch?v=ePj8hx2C-IE)
- [YouTube #5](https://www.youtube.com/watch?v=u6I8HCJlIk0)
- [YouTube #6](https://www.youtube.com/watch?v=l6dx_0LobsA)
- [Notes on parallel computing and big data](https://drive.google.com/drive/u/2/folders/13mzxrofldkbdgF_eT5EPZ1cEiCgOT78d)
***
