

/**
  * Created by markmo on 20/01/2016.
  */
class DemoSpec extends UnitSpec {

  "Data" should "be processed" in {
    val demo = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics.parquet")
    val tx = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Transactions.parquet")
    //val hub = sqlContext.read.load(s"$BASE_URI/$LAYER_ACQUISITION/customer_hub/history.parquet")
  }

}
