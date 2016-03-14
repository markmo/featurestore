import java.util.Calendar

import common.utility.dateFunctions._
import diamond.models.Event
import diamond.transform.eventFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.Duration

/**
  * Created by markmo on 27/02/2016.
  */
class EventAnalysisSpec extends UnitSpec {

  @transient var rawDF: DataFrame = _

  val testDataPath = getClass.getResource("/test_events.csv").getPath

  override def beforeAll(): Unit = {
    super.beforeAll()
    rawDF =
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(testDataPath)

    rawDF.registerTempTable("events")
  }

  "The framework" should "extract paths from event data" in {
    val cal = Calendar.getInstance
    val now = cal.getTime
    cal.set(9999, 11, 1)
    val defaultEndDate = cal.getTime
    val source = testDataPath

    val events: RDD[Event] = sqlContext.sql(
      """
        |select customer_id, event_type, date_time
        |from events
      """.stripMargin)
      .map(row => {
        val eventType = row(1).toString
        Event(entity = row(0).toString,
          eventType = eventType,
          ts = convertStringToDate(row(2).toString, OUTPUT_DATE_TIME_PATTERN),
          namespace = "default",
          session = 0,
          task = None,
          value = eventType,
          properties = "{}",
          startTime = now,
          endTime = defaultEndDate,
          source = source,
          processType = "test",
          processId = "test",
          processDate = now,
          userId = "test",
          rectype = "I",
          version = 1)
      })

    val interactions = events.previousInteractions(3, now)

    val ps = paths(interactions.collect().map {
      case (entity, evs) => (entity, evs.toIterable)
    }.toMap)

    ps("1001") should include ("livechat")

    val uniqueInteractions = events.previousUniqueInteractions("call", 3, now)

    val unique_ps = uniquePaths(uniqueInteractions)

    unique_ps("1002") should equal ("web,call")
  }

  it should "sessionize event data" in {
    val cal = Calendar.getInstance
    val now = cal.getTime
    cal.set(9999, 11, 1)
    val defaultEndDate = cal.getTime
    val source = testDataPath

    val events: RDD[Event] = sqlContext.sql(
      """
        |select customer_id, event_type, date_time
        |from events
      """.stripMargin)
      .map(row => {
        val eventType = row(1).toString
        Event(entity = row(0).toString,
          eventType = eventType,
          ts = convertStringToDate(row(2).toString, OUTPUT_DATE_TIME_PATTERN),
          namespace = "default",
          session = 0,
          task = None,
          value = eventType,
          properties = "{}",
          startTime = now,
          endTime = defaultEndDate,
          source = source,
          processType = "test",
          processId = "test",
          processDate = now,
          userId = "test",
          rectype = "I",
          version = 1)
      })

    val sessionized = events.sessionize(Duration(1 * 24 * 60 * 60 * 1000))

    val churn1003 = sessionized.filter(ev => ev.entity == "1003" && ev.eventType == "churn").first()

    churn1003.session should be (3)
  }

}
