import diamond.io.{CSVSink, CSVSource}
import diamond.store.ErrorRepository
import diamond.transform.TransformationContext
import diamond.transform.row.{AppendColumnRowTransformation, RowTransformation}
import diamond.transform.sql.NamedSQLTransformation
import diamond.transform.table.RowTransformationPipeline
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.io.Path
import scala.util.Try

/**
  * Created by markmo on 12/12/2015.
  */
class TransformSpec extends UnitSpec {

  @transient var rawDF: DataFrame = _

  implicit val errorRepository = new ErrorRepository

  val path = getClass.getResource("events_sample.csv").getPath

  val inputSchema = StructType(
    StructField("entityIdType", StringType) ::
    StructField("entityId", StringType) ::
    StructField("eventType", StringType) ::
    StructField("ts", StringType) ::
    StructField("value", StringType, nullable = true) ::
    StructField("properties", StringType, nullable = true) ::
    StructField("processTime", StringType) :: Nil
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    rawDF =
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .schema(inputSchema)
        .load(path)

    rawDF.registerTempTable("events")
  }

  // define aliases

  val Transform = RowTransformation
  val AppendColumn = AppendColumnRowTransformation

  // import helper functions such as fieldLocator
  import RowTransformation._

  val HelloTransform = Transform(
    name = "Hello"
  ) { (row, ctx) => {
    val f = fieldLocator(row, ctx)
    Row(
      f("entityIdType"),
      f("entityId"),
      s"Hello ${f("eventType")}",
      f("ts"),
      f("value"),
      f("properties"),
      f("processTime")
    )
  }
  }

  val WorldTransform = Transform(
    name = "World"
  ) { (row, ctx) =>
    Row(
      row.getString(0),
      row.getString(1),
      "World " + row.getString(2),
      row.getString(3),
      row.getString(4),
      row.getString(5),
      row.getString(6)
    )
  }

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


  // Tests

  "A CSV file" should "load into a DataFrame" in {
    rawDF.take(3).length should be(3)
  }

  "A Pipeline" should "apply a transformation to the RDD" in {

    // A Pipeline will scan through the entire dataset, performing
    // one or more transformations per row, in the right sequence.
    // This work will be parallelized thanks to Spark.
    val pipeline = new RowTransformationPipeline("test")

    // dependencies can be defined in the Transform object or outside
    // Must be a DAG i.e. no cycles

    HelloTransform.addDependencies(WorldTransform)

    FiftyTransform.addDependencies(HelloTransform)

    // order doesn't matter - will respect dependencies
    pipeline.addTransformations(AddFiveTransform, HelloTransform, WorldTransform, FiftyTransform)

    // create a new context object that can pass state between transformations
    val ctx = new TransformationContext

    // set the schema into the context
    ctx(RowTransformation.SCHEMA_KEY, rawDF.schema)

    // execute the pipeline
    val results = pipeline(rawDF, ctx)

    println {
      pipeline.printDAG()
    }

    // our tests
    val first = results.take(1)(0)
    first.getString(2).startsWith("Hello World") should be (true)
    first.getInt(7) should be (50)
    first.getInt(8) should be (55)
  }

  it should "read from a Source and write to a Sink" in {
    // remove out_path if already exists
    val out = Path("/tmp/workflow_spec_out_csv")
    Try(out.deleteRecursively())

    val source = CSVSource(sqlContext)

    val sink = CSVSink(sqlContext)

    val ctx = new TransformationContext
    ctx("in_path", path)
    ctx("schema", inputSchema)
    ctx("out_path", out.toString)
    ctx("errorThreshold", 1)

    val pipeline = new RowTransformationPipeline("test")
    pipeline.addTransformations(AddFiveTransform, HelloTransform, WorldTransform, FiftyTransform)

    val results = pipeline.run(source, sink, ctx)

    val first = results.take(1)(0)
    first.getString(2).startsWith("Hello World") should be(true)
    first.getInt(7) should be (50)
    first.getInt(8) should be (55)
  }

  "A NamedSQLTransformation" should "read and execute a SQL statement from a properties file given a symbolic name" in {
    val transform = new NamedSQLTransformation("/sql.properties", "query1", Map())

    val results = transform(sqlContext)

    results.collect().length should be > 10
  }


  it should "read and execute a SQL statement from an XML configuration file given a symbolic name" in {
    val transform = new NamedSQLTransformation("/sql.xml", "query1", Map())

    val results = transform(sqlContext)

    results.collect().length should be > 10
  }

}
