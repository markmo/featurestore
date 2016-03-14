package common.parsing

/**
  * Created by markmo on 12/03/2016.
  */
trait Parser[T] {

  def parse(value: Any): Option[T]

}
