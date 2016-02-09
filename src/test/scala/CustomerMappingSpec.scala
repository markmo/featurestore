import diamond.ComponentRegistry
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * Created by markmo on 7/02/2016.
  */
class CustomerMappingSpec extends UnitSpec {

  val parquetLoader = ComponentRegistry.dataLoader
  val customerResolver = ComponentRegistry.customerResolver

  "ParquetDataLoader" should "load customer mappings using Parquet" in {

    val emailMappings = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/email_mappings.parquet")

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
    val demo = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics.parquet")
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

    val hub = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub.parquet")
    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/current.parquet")
    val names = customers.schema.fieldNames.toList
    val joined = customers.join(hub, "entity_id").select(hub("customer_id") :: names.map(customers(_)): _*)
    val sample = joined.filter(col("customer_id").cast(IntegerType) > lit(20000))

    val mapped = customerResolver.mapEntities(sample, entityType = "customer", targetIdType = "email")

    mapped.sort(desc("customer_id")).select("customer_id", "email").take(10).foreach(println)

  }
}
