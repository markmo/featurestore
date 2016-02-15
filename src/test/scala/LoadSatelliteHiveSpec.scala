

/**
  * Created by markmo on 23/01/2016.
  */
class LoadSatelliteHiveSpec extends UnitSpec {

  val hiveLoader = HiveComponentRegistry.dataLoader

  import conf.data._

  "Customers" should "load customers into a satellite table using Hive" in {
    val demoSatConfig = acquisition.satellites("customer-demographics")
    import demoSatConfig._

    val demo = sqlContext.read.load(source)

    hiveLoader.loadSatellite(demo,
      isDelta = isDelta,
      tableName = tableName,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "Load Full",
      processId = "initial",
      userId = "test",
      partitionKeys = partitionKeys,
      newNames = newNames
    )

    val customers = sqlContext.sql(
      """
        |select *
        |from customer_demo
      """.stripMargin)

    customers.count() should be (20000)
  }

  it should "load deltas into a satellite table using Hive" in {
    val demoSatConfig = acquisition.satellites("customer-demographics-delta")
    import demoSatConfig._

    val delta = sqlContext.read.load(source)

    hiveLoader.loadSatellite(delta,
      isDelta = isDelta,
      tableName = tableName,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "Load Delta",
      processId = "delta",
      userId = "test",
      partitionKeys = partitionKeys,
      newNames = newNames
    )

    val customers = sqlContext.sql(
      """
        |select *
        |from customer_demo
      """.stripMargin)

    customers.count() should be (20010)
  }

  it should "perform change data capture using Hive" in {
    val rawSourcePath = raw.tables("demographics-delta-updates").path
    val updates = sqlContext.read.load(rawSourcePath)

    val demoSatConfig = acquisition.satellites("customer-demographics-delta")
    import demoSatConfig._

    hiveLoader.loadSatellite(updates,
      isDelta = isDelta,
      tableName = tableName,
      idFields = idFields,
      idType = idType,
      source = rawSourcePath,
      processType = "Load Delta",
      processId = "updates",
      userId = "test",
      partitionKeys = partitionKeys,
      newNames = newNames
    )

    val customers = sqlContext.sql(
      """
        |select *
        |from customer_demo
      """.stripMargin)

    customers.count() should be (20020)
  }

}
