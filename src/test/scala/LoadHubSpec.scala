import java.net.URI

import diamond.ComponentRegistry
import diamond.utility.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

/**
  * Created by markmo on 23/01/2016.
  */
class LoadHubSpec extends UnitSpec {

  val parquetLoader = ComponentRegistry.dataLoader
  val hiveLoader = HiveComponentRegistry.dataLoader

  import conf.data._

  "Customers" should "be registered into the customer_hub table using Parquet" in {
    val demo = sqlContext.read.load(raw.tables("demographics").path)

    val customerHubConfig = acquisition.hubs("customer")
    import customerHubConfig._

    parquetLoader.loadHub(df = demo,
      isDelta = isDelta,
      entityType = entityType,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "test",
      processId = "test",
      userId = "test",
      newNames = Map(
        "cust_id" -> "customer_id"
      )
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub/history.parquet")

    val schema = customers.schema

    //Parquet writes columns out of order (compared to the schema)
    //https://issues.apache.org/jira/browse/PARQUET-188
    //Fixed in 1.6.0

    val first = customers.take(1)(0)
    val id = first(schema.fieldIndex("customer_id"))
    first(schema.fieldIndex("entity_id")) should equal(hashKey(idType + id.toString))
    first(schema.fieldIndex("id_type")) should equal(idType)
    customers.count() should be (20000)
  }

  it should "be registered into the customer_hub table using Hive" in {
    val demo = sqlContext.read.load(raw.tables("demographics").path)

    val customerHubConfig = acquisition.hubs("customer")
    import customerHubConfig._

    hiveLoader.loadHub(df = demo,
      isDelta = isDelta,
      entityType = entityType,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "test",
      processId = "test",
      userId = "test",
      newNames = newNames
    )

    val customers = sqlContext.sql(
      """
        |select entity_id
        |,customer_id
        |,id_type
        |from customer_hub
      """.stripMargin)

    val schema = customers.schema

    val first = customers.take(1)(0)
    val id = first(schema.fieldIndex("customer_id"))
    first(schema.fieldIndex("entity_id")) should equal(hashKey(idType + id.toString))
    first(schema.fieldIndex("id_type")) should equal(idType)
    customers.count() should be (20000)
  }

  "New Customers" should "be appended to the customer_hub table using Parquet" in {
    val delta = sqlContext.read.load(raw.tables("demographics-delta").path)

    val customerHubConfig = acquisition.hubs("customer")
    import customerHubConfig._

    parquetLoader.loadHub(df = delta,
      isDelta = true,
      entityType = entityType,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "test",
      processId = "test",
      userId = "test",
      newNames = newNames
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub/history.parquet")

    val schema = customers.schema

    val last = customers.sort(desc("customer_id")).take(1)(0)
    val id = last(schema.fieldIndex("customer_id"))
    last(schema.fieldIndex("entity_id")) should equal(hashKey(idType + id.toString))
    customers.count() should be (20010)
  }

  it should "be appended to the customer_hub table using Hive" in {
    val delta = sqlContext.read.load(raw.tables("demographics-delta").path)

    val customerHubConfig = acquisition.hubs("customer")
    import customerHubConfig._

    hiveLoader.loadHub(df = delta,
      isDelta = true,
      entityType = entityType,
      idFields = List("cust_id"),
      idType = idType,
      source = source,
      processType = "test",
      processId = "test",
      userId = "test"
    )

    val customers = sqlContext.sql(
      """
        |select entity_id
        |,customer_id
        |,id_type
        |from customer_hub
      """.stripMargin)

    val schema = customers.schema

    val last = customers.sort(desc("customer_id")).take(1)(0)
    val id = last(schema.fieldIndex("customer_id"))
    last(schema.fieldIndex("entity_id")) should equal(hashKey(idType + id.toString))
    customers.count() should be (20010)
  }

  /*
  override def afterAll(): Unit = {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    fs.delete(new Path(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub"), true)
    super.afterAll()
  }*/

}
