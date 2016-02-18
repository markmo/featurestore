import com.typesafe.config.ConfigFactory
import diamond.AppConfig
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by markmo on 12/12/2015.
  */
abstract class UnitSpec extends FlatSpec with SharedSparkContext with Matchers {

  implicit val conf = new AppConfig(ConfigFactory.load())

  import conf.data._

  val BASE_URI = baseURI
  val LAYER_RAW = raw.path
  val LAYER_ACQUISITION = acquisition.path

}
