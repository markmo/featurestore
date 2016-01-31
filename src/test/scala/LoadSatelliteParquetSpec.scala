import java.net.URI

import diamond.load.ParquetDataLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by markmo on 23/01/2016.
  */
class LoadSatelliteParquetSpec extends UnitSpec {

  val BASE_URI = "hdfs://localhost:9000"

  val parquetLoader = new ParquetDataLoader

  "Customers" should "load customers into a satellite table using Parquet" in {
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

  it should "load deltas into a satellite table using Hive" in {
    val delta = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics_Delta.parquet")

    parquetLoader.loadSatellite(delta,
      isDelta = true,
      tableName = "customer_demo",
      idField = "cust_id",
      idType = "id1",
      partitionKeys = None,
      writeChangeTables = true,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.read.load("hdfs://localhost:9000/il/customer_demo/customer_demo.parquet")

    customers.count() should be (20010)
  }

  it should "perform change data capture using Hive" in {
    val updates = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics_Delta_Updates.parquet")

    parquetLoader.loadSatellite(updates,
      isDelta = true,
      tableName = "customer_demo",
      idField = "cust_id",
      idType = "id1",
      partitionKeys = None,
      writeChangeTables = true,
      newNames = Map(
        "age25to29" -> "age_25_29",
        "age30to34" -> "age_30_34"
      )
    )

    val customers = sqlContext.read.load("hdfs://localhost:9000/il/customer_demo/customer_demo.parquet")

    customers.count() should be (20020)
  }

  /*
  override def afterAll() {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    fs.delete(new Path("/il/customer_demo"), true)
    super.afterAll()
  }*/

}
