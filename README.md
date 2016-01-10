# featurestore

Building blocks and patterns for building data prep transformations in Spark.

For example:

    val Transform = RowTransformation
    val AppendColumn = AppendColumnRowTransformation

    val FiftyTransform = AppendColumn(
        name = "Fifty",
        columnName = "column_7",
        dataType = IntegerType
      ) { (row, ctx) =>
        50
      }
      
    val AddFiveTransform = AppendColumn(
        name = "AddFive",
        columnName = "column_8",
        dataType = IntegerType
      ) { (row, ctx) =>
        row.getInt(7) + 5
      } addDependencies FiftyTransform
          
    import RowTransformation._

    val HelloTransform = Transform(
        name = "Hello"
      ) { (row, ctx) => {
        val f = fieldLocator(row, ctx)
        Row(
          f("entityIdType"),
          f("entityId"),
          s"Hello ${f("attribute")}",
          f("ts"),
          f("value"),
          f("properties"),
          f("processTime")
        )
      }}
      
    val pipeline = new RowTransformationPipeline("test")
    
    FiftyTransform.addDependencies(HelloTransform)
    
    pipeline.addTransformations(AddFiveTransform, HelloTransform, FiftyTransform)
    
    val ctx = new TransformationContext
    ctx(SCHEMA_KEY, rawDF.schema)
    
    // execute the pipeline
    val results = pipeline(rawDF, ctx)

With Sources and Sinks

    val source = CSVSource(sqlContext)
    val sink = CSVSink(sqlContext)
    
    val path = "/tmp/events_sample.csv").getPath

    // Set params in the TransformationContext
    ctx("in_path", path)
    ctx("schema", inputSchema)
    ctx("out_path", "/tmp/workflow_spec_out_csv")

    val results = pipeline.run(source, sink, ctx)

Organizing SQL

sql.properties

    query1 = select attribute, ts, value from events where attribute = '745'
    query2 = select attribute, ts, value from events where entityId = '2017512160'

Alternatively, sql.xml

    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
    <properties>
        <comment>Test queries</comment>
        <entry key="query1">select attribute, ts, value from events where attribute = '745'</entry>
        <entry key="query2">select attribute, ts, value from events where entityId = '2017512160'</entry>
    </properties>

In code

    val transform = new NamedSQLTransformation("<path-to>/sql.properties", "query1")
    
    val results = transform(sqlContext)

    val transform2 = new NamedSQLTransformation("<path-to>/sql.xml", "query1")
    
    val results2 = transform2(sqlContext)

## Dependencies

* Apache Spark 1.5.2
* https://github.com/mdr/ascii-graphs v0.0.3
* Spark-CSV 1.3.0
* https://github.com/tototoshi/scala-csv v1.2.2
* ScalaTest v2.2.4