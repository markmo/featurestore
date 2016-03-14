import java.net.URI

import common.utility.hashFunctions._
import diamond.load.ParquetDataLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

/**
  * Created by markmo on 23/01/2016.
  */
class LoadHubParquetSpec extends UnitSpec {

  val dataLoader = new ParquetDataLoader

  import conf.data._

  "Customers" should "be registered into the customer_hub table using Parquet" in {
    val hubConf = acquisition.hubs("customer")
    import hubConf._

    val demo = sqlContext.read.load(source)

    dataLoader.loadHub(demo,
      isDelta = isDelta,
      entityType = entityType,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "Load Full",
      processId = "initial",
      userId = "test",
      newNames = newNames
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub/history.parquet")

    val schema = customers.schema

    //Parquet writes columns out of order (compared to the schema)
    //https://issues.apache.org/jira/browse/PARQUET-188
    //Fixed in 1.6.0

    val first = customers.take(1)(0)
    val id = first(schema.fieldIndex("customer_id"))
    first(schema.fieldIndex("entity_id")) should equal (hashKey(idType + id.toString))
    first(schema.fieldIndex("id_type")) should equal (idType)
    customers.count() should be (20000)
  }

  "New Customers" should "be appended to the customer_hub table using Parquet" in {
    val delta = sqlContext.read.load(raw.tables("demographics-delta").path)

    val hubConf = acquisition.hubs("customer")
    import hubConf._

    dataLoader.loadHub(delta,
      isDelta = true,
      entityType = entityType,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "Load Delta",
      processId = "delta",
      userId = "test",
      newNames = newNames
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub/history.parquet")

    val schema = customers.schema

    val last = customers.sort(desc("customer_id")).take(1)(0)
    val id = last(schema.fieldIndex("customer_id"))
    last(schema.fieldIndex("entity_id")) should equal (hashKey(idType + id.toString))
    customers.count() should be (20010)
  }

  override def afterAll(): Unit = {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    fs.delete(new Path(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub"), true)
    super.afterAll()
  }

}
