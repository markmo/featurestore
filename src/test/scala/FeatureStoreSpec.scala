import java.util.Calendar

import common.utility.dateFunctions._
import common.utility.hashFunctions._
import diamond.models.{AttributeType, Event, Feature}
import diamond.store.{FeatureStore, FeatureStoreRepository}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by markmo on 12/03/2016.
  */
class FeatureStoreSpec extends UnitSpec {

  @transient var rawDF: DataFrame = _

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

  "A Feature Store Repository" should "save features to a dictionary file and load back in" in {
    import diamond.models.AttributeType._

    val feature1 = Feature("feature1", Base, "test", "string", "Test feature", active = true)
    val feature2 = Feature("feature2", Base, "test", "string", "Test feature", active = true)

    val store = new FeatureStore
    store.registerFeature(feature1)
    store.registerFeature(feature2)

    val repo = new FeatureStoreRepository
    repo.save(store)

    val loadedStore = repo.load()
    val features = loadedStore.registeredFeatures

    features.length should be (2)
    features(0).attribute should equal ("feature1")
  }

  "A Snapshot" should "return a snapshot view of features" in {
    import diamond.transform.PivotFunctions._

    val cal = Calendar.getInstance()

    val events: RDD[Event] = rawDF.map { row =>
      Event(
        hashKey(row.getAs[String]("entityIdType") + row.getAs[String]("entityId")),
        row.getAs[String]("eventType"),
        convertStringToDate(row.getAs[String]("ts"), "yyyy-MM-dd"),
        "test",
        0,
        None,
        row.getAs[String]("value"),
        row.getAs[String]("properties"),
        cal.getTime,
        cal.getTime,
        "events_sample.csv",
        "test",
        "test",
        cal.getTime,
        "test",
        "I",
        1
      )
    }

    val store = new FeatureStore

    store.registerFeature(Feature("745", AttributeType.Base, "test", "string", "Attribute 745", active = true))

    val snap = snapshot(events, cal.getTime, store)

    snap.count() should be(48)

    cal.set(2013, 2, 31)

    val filtered = snap.filter { r =>
      r.head == hashKey("607" + "2016565915")
    }
    filtered.collect()(0)(1) should equal ("2")

    val snap2 = snapshot(events, cal.getTime, store)

    snap2.count() should be(6)

    val filtered2 = snap2.filter { r =>
      r.head == hashKey("607" + "2016565915")
    }

    filtered2.collect()(0)(1) should equal ("1")
  }

}
