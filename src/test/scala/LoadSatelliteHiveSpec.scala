import diamond.load.HiveDataLoader

/**
  * Created by markmo on 23/01/2016.
  */
class LoadSatelliteHiveSpec extends UnitSpec {

  val dataLoader = new HiveDataLoader

  import conf.data._

  "Customers" should "load customers into a satellite table using Hive" in {
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
    val satConf = acquisition.satellites("customer-demographics-delta")
    import satConf._

    val delta = sqlContext.read.load(source)

    dataLoader.loadSatellite(delta,
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

    val satConf = acquisition.satellites("customer-demographics-delta")
    import satConf._

    dataLoader.loadSatellite(updates,
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
