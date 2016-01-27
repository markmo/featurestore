

/**
  * Created by markmo on 20/01/2016.
  */
class DemoSpec extends UnitSpec {

  "Data" should "be processed" in {
    val demo = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Demographics.parquet")
    val tx = sqlContext.read.load("hdfs://localhost:9000/base/Customer_Transactions.parquet")
//    val hub = sqlContext.read.load("hdfs://localhost:9000/il/customer_hub.parquet")
  }

}
