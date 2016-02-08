import java.net.URI
import java.util.Date

import diamond.load.ParquetDataLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
  * Created by markmo on 1/02/2016.
  */
class LoadSatelliteOverwriteParquetSpec extends UnitSpec {

  val BASE_URI = "hdfs://localhost:9000"
  val LAYER_RAW = "base"
  val LAYER_ACQUISITION = "acquisition"

  val parquetLoader = new ParquetDataLoader

  "ParquetDataLoader" should "load customers into a satellite table using Parquet" in {
    val demo = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics.parquet")

    parquetLoader.loadSatellite(demo,
      isDelta = false,
      tableName = "customer_demo",
      idFields = List("cust_id"),
      idType = "id1",
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test",
      partitionKeys = None,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/history.parquet")

    customers.count() should be (20000)
  }

  it should "load deltas into a satellite table using Parquet" in {
    val delta = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics_Delta.parquet")

    parquetLoader.loadSatellite(delta,
      isDelta = true,
      tableName = "customer_demo",
      idFields = List("cust_id"),
      idType = "id1",
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test",
      partitionKeys = None,
      writeChangeTables = true,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/history.parquet")

    customers.count() should be (20010)
  }

  it should "perform change data capture updating changed records by overwriting the Parquet file" in {
    val updates = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics_Delta_Updates.parquet")

    parquetLoader.loadSatellite(updates,
      isDelta = true,
      tableName = "customer_demo",
      idFields = List("cust_id"),
      idType = "id1",
      source = "test",
      processType = "test",
      processId = "test",
      userId = "test",
      partitionKeys = None,
      overwrite = true,
      writeChangeTables = true,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/history.parquet")

    customers.count() should be (20020)

    val hub = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub.parquet")

    val names = customers.schema.fieldNames.toList

    val joined = customers.join(hub, "entity_id").select(hub("customer_id") :: names.map(customers(_)): _*)

    val cust20010 = joined.where("customer_id = '20010'")

    cust20010.count() should be (2)
    cust20010.where("rectype = 'U'").count() should be (2)
    cust20010.where("version = 1").first().getAs[Date]("end_time") should equal(cust20010.where("version = 2").first().getAs[Date]("start_time"))

    val current = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/current.parquet")

    val currentJoined = current.join(hub, "entity_id").select(hub("customer_id") :: names.map(current(_)): _*)

    val current20010 = currentJoined.where("customer_id = '20010'")

    current20010.count() should be (1)

    val first = current20010.first()
    first.getAs[String]("rectype") should equal("U")
    first.getAs[Int]("version") should be (2)
    first.getAs[Long]("age_25_29") should be (1)

    val changed = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/changed.parquet")

    changed.count() should be (10)

    val changedJoined = changed.join(hub, "entity_id").select(hub("customer_id") :: names.map(changed(_)): _*)

    val changed20010 = changedJoined.where("customer_id = '20010'")

    changed20010.count() should be (1)
    val fi = changed20010.first()
    fi.getAs[String]("rectype") should equal("U")
    fi.getAs[Int]("version") should be (2)
    fi.getAs[Long]("age_25_29") should be (1)
  }

  override def afterAll() {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    fs.delete(new Path(s"/$LAYER_ACQUISITION/customer_demo"), true)
    super.afterAll()
  }

}
