Spark WordCount Test Example
========================

Implementation of WordCount on [Spark](http://www.spark-project.org)

# Getting Started #

## Requirements

[Spark 1.3.1](http://www.spark-project.org)
Spark is an open source cluster computing system that aims to make data analytics fast — both fast to run and fast to write.

[sbt 0.11.3](http://www.scala-sbt.org/)
A build tool for Scala and Java projects. It requires Java 1.6 or later.

[Scala 2.9.2](http://www.scala-lang.org/)
Scala is a general purpose programming language designed to express common programming patterns in a concise, elegant, and type-safe way.


## Usage

```bash
# Compile current project
mvn package

# Submit jar to spark
{{SPARK_HOME}}/bin/spark-submit --class org.apache.spark.examples.JavaWordCount target/SparkKMeans-1.0-SNAPSHOT.jar linux.words
```
