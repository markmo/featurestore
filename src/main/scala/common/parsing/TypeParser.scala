package common.parsing

import scala.collection.mutable

/**
  * Created by markmo on 13/03/2016.
  */
class TypeParser {

  private val registry = mutable.Map[Class[_], Parser[_]]()

  def register(key: Class[_], parser: Parser[_]): Unit = registry(key) = parser

  def parse[T](value: Any, clazz: Class[T]): Option[T] = {
    val parser = registry(clazz).asInstanceOf[Parser[T]]
    if (parser == null) {
      None
    } else {
      parser.parse(value)
    }
  }
}
