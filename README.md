![Project Diamond](./images/diamond.png)

# featurestore (Project Diamond)

A framework for feature engineering on Spark that applies the following principles:

* Reactive data pipelines - data from source is "pulled through" a set of transformations to derive a feature, which may involve a machine learning model step (similar to how a spreadsheet recalculates after a cell change). Pipelines are defined through function decomposition instead of through orchestration. When new data becomes available, a message is published, which results in all features that are dependent on the new data to recalculate.
* Feature history is stored as nested data structures, which is supported by storage formats such as Parquet and Cassandra.

![Traditional Data Pipeline](./images/traditional_data_pipeline.png)

Traditionally data is “pushed through” given an updated source and coordinated using a workflow / scheduling tool.

![Reactive Pipeline](./images/reactive_pipeline.png)

A reactive system will publish a message that new data is available, and the reactive system will ”pull data through” to update features just-in-time. This creates a lean yet low latency data pipeline.

![History as nested data structures](./images/nested_history.png)

Using a nested structure, history belongs to the individual feature. History doesn’t need to be recalculated at all if not required for the new feature.

# Ingestion

## Ingestion Philosophy

* Decouple raw ingestion from consumable data
** Move heavy lifting into Spark
** Keep raw data delivery service lean
** Case Study: Uber

## Dependencies

* Scala 2.10
* Apache Spark 1.5.2
* https://github.com/mdr/ascii-graphs v0.0.3
* Spark-CSV 1.3.0
* https://github.com/tototoshi/scala-csv v1.2.2
* [H2O Sparkling Water](https://github.com/h2oai/sparkling-water) v1.5.2
* ScalaTest v2.2.4

## TODO

* Stitching function to combine small files up to HDFS block size
* Possible use of HDFS iNotify event to trigger transformation pipelines for a set of features
* Binary compatibility with transformations in the Spark MLlib Pipeline API
* Consider use of the Datasets API. A Dataset is a new experimental interface added in Spark 1.6 that tries to provide the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL’s optimized execution engine.
* Consider supplying an explicit schema parameter to transformation functions.
* Consider moving library functions to package object.
* Metrics and Restart.
* Improve type safety in TransformationContext.