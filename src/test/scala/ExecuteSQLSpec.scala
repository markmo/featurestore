import java.util.Calendar

import diamond.transform.sql.NamedSQLTransformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by markmo on 23/02/2016.
  */
class ExecuteSQLSpec extends UnitSpec {

  import diamond.utility.functions._

  @transient var rawDF: DataFrame = _

  val path = getClass.getResource("events_sample.csv").getPath

  val inputSchema = StructType(
    StructField("entityIdType", StringType) ::
    StructField("entityId", StringType) ::
    StructField("attribute", StringType) ::
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

  "The Framework" should "load a named SQL statement and execute using parameters from configuration" in {
    // this query is selecting from `events` which is registered above
    val transform = new NamedSQLTransformation("/sql.properties", "query3", parameterize(conf.user))
    val results = transform(sqlContext)

    // results should return 1 row
    results.count() should be (1)

    // and the time should be '2013-02-04 00:00:00+10'
    val cal = Calendar.getInstance
    val eventTimeStr = results.first().getAs[String]("ts")
    val eventTime = convertStringToDate(eventTimeStr, OUTPUT_DATE_TIME_PATTERN)
    cal.setTime(eventTime)
    cal.get(Calendar.DAY_OF_MONTH) should be (4)
  }

}
