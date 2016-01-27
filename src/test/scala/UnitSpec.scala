import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by markmo on 12/12/2015.
  */
abstract class UnitSpec extends FlatSpec with SharedSparkContext with Matchers {

}
