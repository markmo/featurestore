import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by markmo on 12/12/2015.
  */
abstract class UnitSpec extends FlatSpec with SharedSparkContext with Matchers {

  val BASE_URI = "hdfs://localhost:9000"
  val LAYER_RAW = "base"
  val LAYER_ACQUISITION = "acquisition"

}
