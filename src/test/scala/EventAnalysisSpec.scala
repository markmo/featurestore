import java.util.Calendar

import diamond.models.Event
import diamond.transform.eventFunctions._
import diamond.utility.dateFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.Duration

/**
  * Created by markmo on 27/02/2016.
  */
class EventAnalysisSpec extends UnitSpec {

  @transient var rawDF: DataFrame = _

  val samplePath = getClass.getResource("/base/omniture/hit_data.tsv").getPath
  val headerPath = getClass.getResource("/base/omniture/column_headers.tsv").getPath
  val testDataPath = getClass.getResource("/test_events.csv").getPath

  override def beforeAll(): Unit = {
    super.beforeAll()
//    val headers = Source.fromFile(headerPath).getLines.mkString.split("\t")
//    val schema = StructType(headers.map(header => StructField(header, StringType)))
//    rawDF =
//      sqlContext.read
//        .format("com.databricks.spark.csv")
//        .option("header", "false")
//        .option("delimiter", "\t")
//        .schema(schema)
//        .load(samplePath)
    rawDF =
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(testDataPath)

    //rawDF.registerTempTable("omniture")
    rawDF.registerTempTable("events")
  }

  "The framework" should "extract paths from event data" in {
//    val fields = rawDF.schema.fieldNames
    val cal = Calendar.getInstance
    val now = cal.getTime
    cal.set(9999, 11, 1)
    val defaultEndDate = cal.getTime
    val source = samplePath

//    rawDF.take(1)(0).toSeq.zipWithIndex.map {
//      case (v, i) => s"${fields(i)}: $v"
//    }.foreach(println)

//    sqlContext.sql(
//      """
//        |select cust_visid, ip, date_time, first_hit_pagename, pagename
//        |,evar1, evar2, evar3, evar4, evar44, event_list
//        |,geo_city, geo_region, geo_country, geo_zip
//        |from omniture
//      """.stripMargin).show()

//    val uniquePages = sqlContext.sql(
//      """
//        |select distinct cust_visid, pagename
//        |from omniture
//      """.stripMargin)
//    uniquePages.registerTempTable("unique_pages")

//    sqlContext.sql(
//      """
//        |select cust_visid, count(pagename)
//        |from unique_pages
//        |group by cust_visid
//        |order by count(pagename) desc
//      """.stripMargin).show()

//    val visits = sqlContext.sql(
//      """
//        |select date_time, pagename
//        |from omniture
//        |where cust_visid = '9558fa63ea4643209ac2caf53b8fd846'
//        |order by date_time desc
//      """.stripMargin)

//    visits.show()

//    visits.foreach(println)

//    val events: RDD[Event] = sqlContext.sql(
//      """
//        |select cust_visid, pagename, date_time
//        |from omniture
//      """.stripMargin)
//      .distinct()
//      .map(row => {
//        if (row(1) == null) {
//          None
//        } else {
//          val ps = row(1).toString.split(":")
//          if (ps.length > 4) {
//            //(ps(3), ps(4), ps.reverse.head)
//            val eventType = ps(3)
//            Some(Event(entity = row(0).toString,
//              eventType = eventType,
//              ts = convertStringToDate(row(2).toString, OUTPUT_DATE_TIME_PATTERN),
//              namespace = "default",
//              session = None,
//              task = None,
//              value = eventType,
//              properties = "{}",
//              startTime = now,
//              endTime = defaultEndDate,
//              source = source,
//              processType = "test",
//              processId = "test",
//              processDate = now,
//              userId = "test",
//              rectype = "I",
//              version = 1))
//          } else {
//            None
//          }
//        }
//      })
//      .flatMap(x => x)

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

//    interactions.collect().map {
//      case (entity, evs) => (entity, evs.map(_.eventType).mkString(","))
//    }.foreach(println)

    val ps = paths(interactions.collect().map {
      case (entity, evs) => (entity, evs.toIterable)
    }.toMap)

    ps("1001") should include ("livechat")

    val uniqueInteractions = events.previousUniqueInteractions("call", 3, now)

//    uniqueInteractions.map {
//      case (entity, evs) => (entity, evs.map {
//        case (ev, k) => (ev.eventType, k)
//      })
//    }.foreach(println)

    val uniqps = uniquePaths(uniqueInteractions)

    uniqps("1002") should equal("web,call")
  }

  it should "sessionize event data" in {
    val cal = Calendar.getInstance
    val now = cal.getTime
    cal.set(9999, 11, 1)
    val defaultEndDate = cal.getTime
    val source = samplePath

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

//    sessionized.collect().map(ev => (ev.entity, ev.session, ev.eventType, ev.ts)).foreach(println)

    val churn1003 = sessionized.filter(ev => ev.entity == "1003" && ev.eventType == "churn").first()

    churn1003.session should be (3)
  }

}
