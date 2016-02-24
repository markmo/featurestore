# Executing SQL queries

While SQL statements can be inlined, it usually makes sense to move into configuration. The benefits include easier maintenance and the ability to use SQL editors for syntax highlighting. The framework supports:

1. A SQL statement in its own file
1. Named SQL statements in a properties file
1. Named SQL statements in an XML file

This example uses named SQL statements in a properties file.

The framework also supports query parameters, passed to the transformation object as a map of key value pairs. Parameters can also be added from configuration, which makes sense for environment variables. A `user` namespace in the configuration settings can be used for this purpose.

## Example

The SQL properties file is put in the classpath. It is then bundled with the application JAR file.

sql.properties

    query3 = select attribute, ts, value from $mytable where entityId = '$entityId'
    ...
    
Static parameters are put in a configuration file (also in the classpath) under the `user` namespace.

application.conf

    include "user.conf"
    ...
    
user.conf

    user {
      mytable = events
      entity-id = 2017512160
    }

Transformations are organised into transformation objects to facilitate dependency management and error handling and reporting. See [Main README]().

See [/src/test/scala/ExecuteSQLSpec]()

    val rawDF =
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .schema(inputSchema)
        .load(path)

    rawDF.registerTempTable("events")
    
    "The Framework" should "load a named SQL statement and execute using parameters from configuration" in {
        // this query is selecting from `events` which is registered above

        val transform = new NamedSQLTransformation("/sql.properties", "query3", parameterize(conf.user))

        val results = transform(sqlContext)
        
        // results should return 1 row
        results.count() should be (1)
    }

(The `parameterize` function is simply converting a map of format { some-key: anyval } from the user configuration to { someKey: str }, which matches the variable format used in the SQL.)

Other parameters could be added to those from configuration in the method call.

The signature of `NamedSQLTransformation` is:

    NamedSQLTransformation(propsPath: String,           // Path to SQL properties file (relative to classpath)
                           name: String,                // Name of query
                           params: Map[String, String]) // Dynamic variables to pass into query

