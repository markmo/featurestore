![Project Diamond](./images/diamond.png)

# featurestore (Project Diamond)

A framework for feature engineering using Spark. Capabilities include:

* [Transformation framework](./docs/transformations.md) for building data preparation pipelines using function composition (src/main/scala/diamond/transform/)
* [Change Data Capture](./src/main/scala/diamond/load/README.md) functionality to determine changes from fresh data feeds and ensure no duplicates (src/main/scala/diamond/load/)
* Event feature engineering (./src/main/scala/diamond/transform/eventFunctions) for analyzing interaction timelines and [customer journey mapping](./docs/customer_journey_mapping.md)
* Creating a flexible, shared Feature Store (src/main/scala/diamond/store/)
* Data presentation including generation of star schemas for visual applications (src/main/scala/star/)
* Utilities for extracting metadata from raw delimited files (src/main/scala/common/inference/)

Further documentation can be founds under docs/, and in source directories.

A working data model can be found under model/.

## Setting up a test environment

The test suite can be run with `sbt test`. However, many of the tests require access to Hadoop. The following instructions are for setting up a local Hadoop instance on OS X.

Hadoop can be installed using [Homebrew](http://brew.sh/).

    brew install hadoop

Hadoop will be installed under /usr/local/Cellar/hadoop/.

Edit the following files under /usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/. Examples are included in env/.

**/usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/hadoop-env.sh**

Find the line with

    export HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"

and change it to

    export HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true -Djava.security.krb5.realm= -Djava.security.krb5.kdc="

**/usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/core-site.xml**

    <property>
        <name>hadoop.tmp.dir</name>
        <value>/usr/local/Cellar/hadoop/hdfs/tmp</value>
        <description>A base for other temporary directories.</description>
    </property>
    <property>
        <name>fs.default.name</name>                                     
        <value>hdfs://localhost:9000</value>                             
    </property>

**/usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/mapred-site.xml**

My be blank by default.

    <configuration>
        <property>
            <name>mapred.job.tracker</name>
            <value>localhost:9010</value>
        </property>
    </configuration>

**/usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/hdfs-site.xml**

    <configuration>
        <property>
            <name>dfs.replication</name>
            <value>1</value>
        </property>
    </configuration>

To simplify Hadoop startup and shutdown, add the following aliases to your ~/.bash_profile script

    alias hstart="/usr/local/Cellar/hadoop/2.6.0/sbin/start-dfs.sh;/usr/local/Cellar/hadoop/2.6.0/sbin/start-yarn.sh"
    alias hstop="/usr/local/Cellar/hadoop/2.6.0/sbin/stop-yarn.sh;/usr/local/Cellar/hadoop/2.6.0/sbin/stop-dfs.sh"

and execute

    source ~/.bash_profile

Before running Hadoop for the first time, format HDFS

    hdfs namenode -format

Starting Hadoop requires ssh login to localhost. Nothing need be done if you have previously generated ssh keys. You can verify by checking the existence of ~/.ssh/id_rsa and ~/.ssh/id_rsa.pub. If not the keys can be generated using

    ssh-keygen -t rsa

Enable remote login

_“System Preferences” -> “Sharing”. Check “Remote Login”_

To avoid retyping passwords every time you startup and shutdown Hadoop, add your key to the list of authorized keys.

     cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

You can verify the last step

    $ ssh localhost
    Last login: Fri Mar  6 20:30:53 2015
    $ exit

To start Hadoop

    hstart

and to stop

    hstop

Copy the test data to HDFS

    hadoop fs -mkdir /base
    
    hadoop fs -put <this-project-root>/src/test/resources/base /base

Setup Hive

    brew install hive

Hive will be installed under /usr/local/Cellar/hive/.

The Hive configuration files are located under /usr/local/Cellar/hive/1.2.1/libexec/conf/.

A working hive-site.xml configuration is located under env/.

Hadoop must be restarted after changing any Hive configuration.

If you're still being asked to enter a password when starting or stopping Hadoop, try

    $ chmod go-w ~/
    $ chmod 700 ~/.ssh
    $ chmod 600 ~/.ssh/authorized_keys

## Dependencies

* [Scala 2.10](http://www.scala-lang.org/)
* [Apache Spark 1.5.2](http://spark.apache.org/docs/1.5.2/)
* [spark-csv 1.3.0](https://github.com/databricks/spark-csv)
* [scala-csv v1.2.2](https://github.com/tototoshi/scala-csv)
* [ascii-graphs v0.0.3](https://github.com/mdr/ascii-graphs)
* [H2O Sparkling Water v1.5.2](https://github.com/h2oai/sparkling-water)
* [ScalaTest v2.2.4](http://www.scalatest.org/)

## TODO

* Stitching function to combine small files up to HDFS block size
* Possible use of HDFS iNotify event to trigger transformation pipelines for a set of features
* Binary compatibility with transformations in the Spark MLlib Pipeline API
* Consider use of the Datasets API. A Dataset is a new experimental interface added in Spark 1.6 that tries to provide the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL’s optimized execution engine.
* Consider supplying an explicit schema parameter to transformation functions.
* Consider moving library functions to package object.
* Metrics and Restart.
* Improve type safety in TransformationContext.