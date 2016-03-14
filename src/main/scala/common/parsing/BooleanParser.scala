package common.parsing

/**
  * Created by markmo on 12/03/2016.
  */
class BooleanParser extends Parser[Boolean] {

  def parse(value: Any): Option[Boolean] = value match {
    case null => None
    case Lower(Trimmed(Str("true" | "t" | "yes" | "y" | "on" | "1"))) => Some(true)
    case Lower(Trimmed(Str("false" | "f" | "no" | "n" | "off" | "0"))) => Some(false)
    case _ => None
  }
}

object Lower {
  def unapply(str: String): Option[String] = Some(str.toLowerCase)
}

object Trimmed {
  def unapply(str: String): Option[String] = Some(str.trim)
}

object Str {
  def unapply(value: Any): Option[String] = Some(value.toString)
}
