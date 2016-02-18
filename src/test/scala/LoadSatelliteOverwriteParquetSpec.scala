import java.net.URI
import java.util.Date

import diamond.load.ParquetDataLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by markmo on 1/02/2016.
  */
class LoadSatelliteOverwriteParquetSpec extends UnitSpec {

  val dataLoader = new ParquetDataLoader

  import conf.data._

  "ParquetDataLoader" should "load customers into a satellite table using Parquet" in {
    val satConf = acquisition.satellites("customer-demographics")
    import satConf._

    val demo = sqlContext.read.load(source)

    dataLoader.loadSatellite(demo,
      isDelta = isDelta,
      tableName = tableName,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "Load Full",
      processId = "initial",
      userId = "test",
      partitionKeys = partitionKeys,
      overwrite = true,
      newNames = newNames
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/history.parquet")

    customers.count() should be (20000)
  }

  it should "load deltas into a satellite table using Parquet" in {
    val satConf = acquisition.satellites("customer-demographics-delta")
    import satConf._

    val delta = sqlContext.read.load(source)

    dataLoader.loadSatellite(delta,
      isDelta = false,
      tableName = tableName,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "Load Delta",
      processId = "delta",
      userId = "test",
      partitionKeys = partitionKeys,
      newNames = newNames,
      overwrite = true,
      writeChangeTables = true
    )

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/history.parquet")

    customers.count() should be (20010)
  }

  it should "perform change data capture updating changed records by overwriting the Parquet file" in {
    val rawSourcePath = raw.tables("demographics-delta-updates").path
    val updates = sqlContext.read.load(rawSourcePath)

    val satConf = acquisition.satellites("customer-demographics-delta")
    import satConf._

    dataLoader.loadSatellite(updates,
      isDelta = false,
      tableName = tableName,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "Load Delta",
      processId = "updates",
      userId = "test",
      partitionKeys = partitionKeys,
      newNames = newNames,
      overwrite = true,
      writeChangeTables = true
    )

    val hubConf = acquisition.hubs("customer")
    val demo = sqlContext.read.load(hubConf.source)
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

    val customers = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_demo/history.parquet")

    customers.count() should be (20020)

    val hub = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub/history.parquet")

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

  override def afterAll(): Unit = {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    fs.delete(new Path(s"/$LAYER_ACQUISITION/customer_demo"), true)
    fs.delete(new Path(s"/$LAYER_ACQUISITION/customer_hub"), true)
    super.afterAll()
  }

}
