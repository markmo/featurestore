import java.net.URI

import diamond.load.{HiveDataLoader, ParquetDataLoader}
import diamond.utility.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

/**
  * Created by markmo on 23/01/2016.
  */
class LoadHubSpec extends UnitSpec {

  val BASE_URI = "hdfs://localhost:9000"
  val LAYER_RAW = "base"
  val LAYER_ACQUISITION = "acquisition"

  val parquetLoader = new ParquetDataLoader
  val hiveLoader = new HiveDataLoader

  "Customers" should "be registered into the customer_hub table using Parquet" in {
    val demo = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics.parquet")

    parquetLoader.loadHub(df = demo,
      isDelta = false,
      entityType = "customer",
      idFields = List("cust_id"),
      idType = "id1",
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test"
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub.parquet")

    val schema = customers.schema

    //Parquet writes columns out of order (compared to the schema)
    //https://issues.apache.org/jira/browse/PARQUET-188
    //Fixed in 1.6.0

    val first = customers.take(1)(0)
    val id = first(schema.fieldIndex("customer_id"))
    first(schema.fieldIndex("entity_id")) should equal(hashKey("id1" + id.toString))
    first(schema.fieldIndex("customer_id_type")) should equal("id1")
    customers.count() should be (20000)
  }

  it should "be registered into the customer_hub table using Hive" in {
    val demo = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics.parquet")

    hiveLoader.loadHub(df = demo,
      isDelta = false,
      entityType = "customer",
      idFields = List("cust_id"),
      idType = "id1",
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test"
    )

    val customers = sqlContext.sql(
      """
        |select entity_id
        |,customer_id
        |,customer_id_type
        |from customer_hub
      """.stripMargin)

    val schema = customers.schema

    val first = customers.take(1)(0)
    val id = first(schema.fieldIndex("customer_id"))
    first(schema.fieldIndex("entity_id")) should equal(hashKey("id1" + id.toString))
    first(schema.fieldIndex("customer_id_type")) should equal("id1")
    customers.count() should be (20000)
  }

  "New Customers" should "be appended to the customer_hub table using Parquet" in {
    val delta = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics_Delta.parquet")

    parquetLoader.loadHub(df = delta,
      isDelta = true,
      entityType = "customer",
      idFields = List("cust_id"),
      idType = "id1",
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test"
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub.parquet")

    val schema = customers.schema

    val last = customers.sort(desc("customer_id")).take(1)(0)
    val id = last(schema.fieldIndex("customer_id"))
    last(schema.fieldIndex("entity_id")) should equal(hashKey("id1" + id.toString))
    customers.count() should be (20010)
  }

  it should "be appended to the customer_hub table using Hive" in {
    val delta = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics_Delta.parquet")

    hiveLoader.loadHub(df = delta,
      isDelta = true,
      entityType = "customer",
      idFields = List("cust_id"),
      idType = "id1",
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test"
    )

    val customers = sqlContext.sql(
      """
        |select entity_id
        |,customer_id
        |,customer_id_type
        |from customer_hub
      """.stripMargin)

    val schema = customers.schema

    val last = customers.sort(desc("customer_id")).take(1)(0)
    val id = last(schema.fieldIndex("customer_id"))
    last(schema.fieldIndex("entity_id")) should equal(hashKey("id1" + id.toString))
    customers.count() should be (20010)
  }

  /*
  override def afterAll() {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    fs.delete(new Path(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub.parquet"), true)
    super.afterAll()
  }*/

}
