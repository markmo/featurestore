import diamond.load.ParquetDataLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * Created by markmo on 7/02/2016.
  */
class CustomerMappingSpec extends UnitSpec {

  val parquetLoader = new ParquetDataLoader

  "ParquetDataLoader" should "load customer mappings using Parquet" in {

    val emailMappings = sqlContext.read.load("hdfs://localhost:9000/base/email_mappings.parquet")

    parquetLoader.loadMapping(emailMappings,
      isDelta = false,
      entityType = "customer",
      idFields1 = List("cust_id"), idType1 = "Customer Number",
      idFields2 = List("email"), idType2 = "email",
      confidence = 1.0,
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test")

    /*
    val demo = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics.parquet")
    parquetLoader.loadSatellite(demo,
      isDelta = false,
      tableName = "customer_demo",
      idFields = List("cust_id"),
      idType = "id1",
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test"
    )*/

    val hub = sqlContext.read.load("hdfs://localhost:9000/il/customer_hub.parquet")
    val customers = sqlContext.read.load("hdfs://localhost:9000/il/customer_demo/current.parquet")
    val names = customers.schema.fieldNames.toList
    val joined = customers.join(hub, "entity_id").select(hub("customer_id") :: names.map(customers(_)): _*)
    val sample = joined.filter(col("customer_id").cast(IntegerType) > lit(20000))

    val mapped = parquetLoader.mapEntities(sample, entityType = "customer", targetIdType = "email")

    mapped.sort(desc("customer_id")).select("customer_id", "email").take(10).foreach(println)

  }
}
