import diamond.utility.stringFunctions._

/**
  * Created by markmo on 12/03/2016.
  */
class StringUtilsSpec extends UnitSpec {

  "A StringTemplate" should "correctly interpolate a string" in {

    "Hello $w".template(Map("w" -> "World")) should equal ("Hello World")
    "Hello ${w}".template(Map("w" -> "World")) should equal ("Hello World")
    "Hello $$w".template(Map("w" -> "World")) should equal ("Hello $$w")
    "Hello $${w}".template(Map("w" -> "World")) should equal ("Hello $${w}")
    "Hello ${}".template(Map("w" -> "World")) should equal ("Hello ${}")
    "Hello ${foo}".template(Map("w" -> "World")) should equal ("Hello ${foo}")
    "Hello ${".template(Map("w" -> "World")) should equal ("Hello ${")
    "Hello $".template(Map("w" -> "World")) should equal ("Hello $")
  }

}
