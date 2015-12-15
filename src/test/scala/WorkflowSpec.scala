import diamond.transformation.{AppendColumnTransformation, Pipeline, TransformationContext, Transformation}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import diamond.transformation.schemas._

/**
  * Created by markmo on 12/12/2015.
  */
class WorkflowSpec extends UnitSpec {

  val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val path = getClass.getResource("/events_sample.csv").getPath

  val rawDF =
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(inputSchema)
      .load(path)

  "A CSV file" should "load into a DataFrame" in {
    rawDF.take(3).length should be (3)
  }

  "A Pipeline" should "apply a transformation to the RDD" in {

    // A Pipeline will scan through the entire dataset, performing
    // one or more transformations in the right sequence per row.
    // This work will be parallelized thanks to Spark.
    val pipeline = new Pipeline

    // dependencies can be defined in the Transform object or outside

    HelloTransform.addDependencies(WorldTransform)

    FiftyTransform.addDependencies(HelloTransform)

    pipeline.addTransformations(AddFiveTransform, HelloTransform, WorldTransform, FiftyTransform)

    // create a new context object that can pass state between transformations
    val ctx = new TransformationContext

    // set the schema into the context
    ctx(Transformation.SCHEMA_KEY, rawDF.schema)

    // execute the pipeline
    val results = pipeline(rawDF, ctx)

    // print some results
    results.take(3).foreach(println)

    println(pipeline.printDAG())

    // our tests
    results.take(1)(0).getString(2).startsWith("Hello World") should be (true)

    results.take(1)(0).getInt(7) should be (50)

    results.take(1)(0).getInt(8) should be (55)
  }

}

/**
  * Need to be careful with these transforms as they can drop columns
  * appended by AppendColumnTransformations if done out of sequence.
  */
object HelloTransform extends Transformation {

  // A name is required for reporting and debugging purposes
  val name = "Hello"

  /**
    * This is where the transformation logic goes.
    *
    * using the fieldLocator helper
    *
    * @param row a spark.sql.Row
    * @param ctx a TransformationContext
    * @return the new spark.sql.Row
    */
  def apply(row: Row, ctx: TransformationContext) = {
    val f = fieldLocator(row, ctx)
    Row(
      f("entityIdType"),
      f("entityId"),
      s"Hello ${f("attribute")}",
      f("ts"),
      f("namespace"),
      f("value"),
      f("properties")
    )
  }

}

object WorldTransform extends Transformation {

  val name = "World"

  // using the built-in Row methods
  def apply(row: Row, ctx: TransformationContext) =
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

/**
  * This type of transform appends a new column to the end
  * so the developer doesn't need to construct a full Row.
  *
  * Implement "append" instead of "apply".
  */
object FiftyTransform extends AppendColumnTransformation {

  val name = "Fifty"

  def append(row: Row, ctx: TransformationContext) = 50

}

object AddFiveTransform extends AppendColumnTransformation {

  val name = "AddFive"

  def append(row: Row, ctx: TransformationContext) = row.getInt(7) + 5

  addDependencies(FiftyTransform)

}
