import java.net.URI

import diamond.load.{HiveDataLoader, ParquetDataLoader}
import diamond.utility.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

/**
  * Created by markmo on 23/01/2016.
  */
class LoadSpec extends UnitSpec {

  val BASE_URI = "hdfs://localhost:9000"

  val CUSTOMER_HUB_PATH = "/il/customer_hub.parquet"

  val parquetLoader = new ParquetDataLoader
  val hiveLoader = new HiveDataLoader

  "Customers" should "be registered into the customer_hub table using Parquet" in {
    val demo = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics.parquet")

    parquetLoader.registerCustomers(demo, "cust_id", "id1")

    val customers = sqlContext.read.load("hdfs://localhost:9000/il/customer_hub.parquet")

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
    val demo = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics.parquet")

    hiveLoader.registerCustomers(demo, "cust_id", "id1")

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

  it should "load customers into a satellite table using Parquet" in {
    val demo = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics.parquet")

    parquetLoader.loadSatellite(demo,
      isDelta = false,
      tableName = "customer_demo",
      idField = "cust_id",
      idType = "id1",
      partitionKeys = None,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.read.load("hdfs://localhost:9000/il/customer_demo/customer_demo.parquet")

    customers.count() should be (20000)
  }

  it should "load customers into a satellite table using Hive" in {
    val demo = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics.parquet")

    hiveLoader.loadSatellite(demo,
      isDelta = false,
      tableName = "customer_demo",
      idField = "cust_id",
      idType = "id1",
      partitionKeys = None,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.sql(
      """
        |select *
        |from customer_demo
      """.stripMargin)

    customers.count() should be (20000)
  }

  it should "load deltas into a satellite table using Hive" in {
    val delta = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics_Delta.parquet")

    hiveLoader.loadSatellite(delta,
      isDelta = false,
      tableName = "customer_demo",
      idField = "cust_id",
      idType = "id1",
      partitionKeys = None,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.sql(
      """
        |select *
        |from customer_demo
      """.stripMargin)

    customers.count() should be (20010)
  }

  it should "perform change data capture using Hive" in {
    val updates = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics_Delta_Updates.parquet")

    hiveLoader.loadSatellite(updates,
      isDelta = false,
      tableName = "customer_demo",
      idField = "cust_id",
      idType = "id1",
      partitionKeys = None,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.sql(
      """
        |select *
        |from customer_demo
      """.stripMargin)

    customers.count() should be (20020)
  }

  "New Customers" should "be appended to the customer_hub table using Parquet" in {
    val delta = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics_Delta.parquet")

    parquetLoader.registerCustomers(delta, "cust_id", "id1")

    val customers = sqlContext.read.load("hdfs://localhost:9000/il/customer_hub.parquet")

    val schema = customers.schema

    val last = customers.sort(desc("customer_id")).take(1)(0)
    val id = last(schema.fieldIndex("customer_id"))
    last(schema.fieldIndex("entity_id")) should equal(hashKey("id1" + id.toString))
    customers.count() should be (20010)
  }

  it should "be appended to the customer_hub table using Hive" in {
    val delta = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics_Delta.parquet")

    hiveLoader.registerCustomers(delta, "cust_id", "id1")

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

  override def afterAll(): Unit = {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    val path = new Path(CUSTOMER_HUB_PATH)
    fs.delete(path, true)
    super.afterAll()
  }

}
