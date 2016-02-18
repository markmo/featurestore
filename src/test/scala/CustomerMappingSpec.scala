import java.net.URI

import diamond.load.{CustomerResolver, ParquetDataLoader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * Created by markmo on 7/02/2016.
  */
class CustomerMappingSpec extends UnitSpec {

  val dataLoader = new ParquetDataLoader
  val customerResolver = new CustomerResolver()(dataLoader)

  import conf.data._

  "ParquetDataLoader" should "load customer mappings using Parquet" in {
    val mapConf = acquisition.mappings("email")
    import mapConf._

    val emailMappings = sqlContext.read.load(source)

    dataLoader.loadMapping(emailMappings,
      isDelta = isDelta,
      entityType = entityType,
      idFields1 = idFields1, idType1 = idType1,
      idFields2 = idFields2, idType2 = idType2,
      confidence = confidence,
      source = source,
      processType = "Load Full",
      processId = "initial",
      userId = "test")

    val demoConf = acquisition.satellites("customer-demographics")
    val demo = sqlContext.read.load(demoConf.source)
    demo.cache()
    dataLoader.loadSatellite(demo,
      isDelta = demoConf.isDelta,
      tableName = demoConf.tableName,
      idFields = demoConf.idFields,
      idType = demoConf.idType,
      source = demoConf.source,
      processType = "Load Full",
      processId = "initial",
      userId = "test"
    )
    val hubConf = acquisition.hubs("customer")
    dataLoader.loadHub(demo,
      isDelta = hubConf.isDelta,
      entityType = hubConf.entityType,
      idFields = hubConf.idFields,
      idType = hubConf.idType,
      source = hubConf.source,
      processType = "Load Full",
      processId = "initial",
      userId = "test",
      newNames = hubConf.newNames
    )
    val delta = sqlContext.read.load(raw.tables("demographics-delta").path)
    dataLoader.loadHub(delta,
      isDelta = true,
      entityType = hubConf.entityType,
      idFields = hubConf.idFields,
      idType = hubConf.idType,
      source = hubConf.source,
      processType = "Load Delta",
      processId = "delta",
      userId = "test",
      newNames = hubConf.newNames
    )

    val hub = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub/history.parquet")
    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/current.parquet")
    val names = customers.schema.fieldNames.toList
    val joined = customers.join(hub, "entity_id").select(hub("customer_id") :: names.map(customers(_)): _*)
    val sample = joined.filter(col("customer_id").cast(IntegerType) > lit(20000))

    val mapped = customerResolver.mapEntities(sample, entityType = "customer", targetIdType = "email")

    mapped.sort(desc("customer_id")).select("customer_id", "email").take(10).foreach(println)

  }

  override def afterAll(): Unit = {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    fs.delete(new Path(s"/$LAYER_ACQUISITION/customer_mapping"), true)
    fs.delete(new Path(s"/$LAYER_ACQUISITION/customer_demo"), true)
    fs.delete(new Path(s"/$LAYER_ACQUISITION/customer_hub"), true)
    super.afterAll()
  }
}
