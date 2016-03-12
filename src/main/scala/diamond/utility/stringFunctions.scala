package diamond.utility

import scala.annotation.tailrec

/**
  * Created by markmo on 27/02/2016.
  */
object stringFunctions {

  /**
    * Turns a string of format "foo_bar" or "foo-bar" into camel case "FooBar"
    *
    * @param str String
    */
  def camelize(str: String, startUpper: Boolean = false): String = {
    def loop(x: List[Char]): List[Char] = x match {
      case ('_' | '-') :: ('_' | '-') :: rest => loop('_' :: rest)
      case ('_' | '-') :: c :: rest => Character.toUpperCase(c) :: loop(rest)
      case ('_' | '-') :: Nil => Nil
      case c :: rest => c :: loop(rest)
      case Nil => Nil
    }
    if (str == null) {
      ""
    } else if (startUpper) {
      loop('_' :: str.toList).mkString
    } else {
      loop(str.toList).mkString
    }
  }

  /**
    * Convert map of format { some-key: anyval } to { someKey: str }
    *
    * @param conf Map[String, Any]
    * @return Map[String, String]
    */
  def parameterize(conf: Map[String, Any]): Map[String, String] = conf map {
    case (k, v) => (camelize(k), v.toString)
  }

  implicit class RichString(val str: String) extends AnyVal {

    def isNumber =
      str.matches(s"""[+-]?((\d+(e\d+)?[lL]?)|(((\d+(\.\d*)?)|(\.\d+))(e\d+)?[fF]?))""")

  }

  /**
    * Using the "pimp my library" pattern.
    *
    * @param str String template
    */
  implicit class StringTemplate(val str: String) extends AnyVal {

    /**
      * Render a string template substituting variables.
      *
      * @param vars Map of variables to replace
      * @return String
      */
    def template(vars: Map[String, String]): String = {
      val sb = new StringBuilder
      val pattern = """^('\$([^']*)'|"\$([^"]*)"|'\$\{([^']*)}'|"\$\{([^"]*)}"|\$\{([^}]*)}|\$([^\s]*)\s|\$([^\s]*)$).*""".r

      @tailrec
      def loop(str: String): String = str match {
        case x if x.isEmpty => sb.toString
        case pattern(x, a, b, c, d, e, f, g) =>
          if (a != null) {
            // '$a'
            val replacement = vars.getOrElse(a, s"$$a")
            sb.append("'").append(replacement).append("'")
            loop(str.substring(x.length))
          } else if (b != null) {
            // "$b"
            val replacement = vars.getOrElse(b, s"$$b")
            sb.append('"').append(replacement).append('"')
            loop(str.substring(x.length))
          } else if (c != null) {
            // '${c}'
            val replacement = vars.getOrElse(c, s"$${c}")
            sb.append("'").append(replacement).append("'")
            loop(str.substring(x.length))
          } else if (d != null) {
            // "${d}"
            val replacement = vars.getOrElse(c, s"$${d}")
            sb.append('"').append(replacement).append('"')
            loop(str.substring(x.length))
          } else if (e != null) {
            // ${e}
            val replacement = vars.getOrElse(e, s"$${$e}")
            sb.append(replacement)
            loop(str.substring(x.length))
          } else if (f != null) {
            // $f\s
            val replacement = vars.getOrElse(f, s"$$$f")
            sb.append(replacement).append(" ")
            loop(str.substring(x.length))
          } else if (g != null) {
            // $e<end-of-string>
            val replacement = vars.getOrElse(g, s"$$$g")
            sb.append(replacement)
            loop(str.substring(x.length))
          } else {
            sb.append(str.head)
            loop(str.tail)
          }
        case _ =>
          sb.append(str.head)
          loop(str.tail)
      }

      loop(str)
    }

  }

}
