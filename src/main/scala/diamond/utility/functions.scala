package diamond.utility

import java.io.FileInputStream
import java.math.BigInteger
import java.net.URI
import java.nio.charset.CodingErrorAction
import java.security.MessageDigest
import java.text.SimpleDateFormat

import diamond.AppConfig
import hex.genmodel.GenModel
import net.openhft.hashing.LongHashFunction
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.annotation.tailrec
import scala.collection.Iterator
import scala.io.{Codec, Source}

/**
  * Created by markmo on 30/11/2015.
  */
object functions {

  val OUTPUT_DATE_PATTERN = "yyyy-MM-dd"

  val OUTPUT_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss"

  /**
    * Converts a date string of a given pattern to a Date object.
    *
    * @param str String date to convert
    * @param pattern String format of date string to parse
    * @return Date
    */
  def convertStringToDate(str: String, pattern: String) = {
    val format = new SimpleDateFormat(pattern)
    format.parse(str)
  }

  /**
    * Formats a date string of a given pattern to a conformed format (yyyy-MM-dd).
    *
    * @param str String date to format
    * @param pattern String format of date string to parse
    * @return String formatted date (yyyy-MM-dd)
    */
  def formatDateString(str: String, pattern: String) = {
    val dt = convertStringToDate(str, pattern)
    val outputFormat = new SimpleDateFormat(OUTPUT_DATE_PATTERN)
    outputFormat.format(dt)
  }

  /**
    * Formats a date string of a given pattern to a conformed date and time format (yyyy-MM-dd HH:mm:ss).
    *
    * @param str String date to format
    * @param pattern String format of date string to parse
    * @return String formatted date and time (yyyy-MM-dd HH:mm:ss)
    */
  def formatDateTimeString(str: String, pattern: String) = {
    val dt = convertStringToDate(str, pattern)
    val outputFormat = new SimpleDateFormat(OUTPUT_DATE_TIME_PATTERN)
    outputFormat.format(dt)
  }

  /**
    * Converts a date string of a given pattern to epoch (unix) time.
    *
    * Defined as the number of seconds that have elapsed since 00:00:00 Coordinated Universal Time (UTC), Thursday, 1 January 1970, not counting leap seconds.
    *
    * @param str String date to parse
    * @param pattern String format of date string to parse
    * @return long epoch (unix) time
    */
  def convertStringToTimestamp(str: String, pattern: String) = {
    convertStringToDate(str, pattern).getTime
  }

  /**
    * Hashes a string key using SHA-256.
    *
    * Used to hash entity keys, which may be composite.
    *
    * @param key String
    * @return String hashed key
    */
  def hashKey(key: String) = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(key.getBytes("UTF-8"))
    val digest = md.digest()
    String.format("%064x", new BigInteger(1, digest))
  }

  /**
    * Fast hashing for change data capture.
    * Uses the xxHash algorithm.
    *
    * @see https://github.com/OpenHFT/Zero-Allocation-Hashing
    * @param str String
    * @return String hashed
    */
  def fastHash(str: String) =
    LongHashFunction.xx_r39().hashChars(str).toString

  /**
    * Translates an EBCDIC file into readable lines.
    *
    * Extended Binary Coded Decimal Interchange Code (EBCDIC) is an eight-bit
    * character encoding used mainly on IBM mainframe and IBM mid-range
    * computer operating systems. EBCDIC descended from the code used with
    * punched cards.
    *
    * This function will report an error if the input is malformed or a
    * character cannot be mapped.
    *
    * @param path String file path
    * @return Iterator[String] of readable lines
    */
  def localReadEBCDIC(path: String): Iterator[String] = {
    val codec = Codec("ibm500")
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)

    Source.fromInputStream(new FileInputStream(path))(codec).getLines
  }

  /**
    *
    * @param path String file path
    * @param conf AppConfig config object
    * @return Iterator[String] of readable lines
    */
  def hdfsReadEBCDIC(path: String)(implicit conf: AppConfig): Iterator[String] = {
    val fs = FileSystem.get(new URI(conf.data.baseURI), new Configuration())
    val codec = Codec("ibm500")
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)

    Source.fromInputStream(fs.open(new Path(path)))(codec).getLines
  }

  /**
    * Turns a string of format "foo_bar" or "foo-bar" into camel case "FooBar"
    *
    * @param str String
    */
  def camelize(str: String, startUpper: Boolean = false): String = {
    def loop(x: List[Char]): List[Char] = x match {
      case ('_'|'-') :: ('_'|'-') :: rest => loop('_' :: rest)
      case ('_'|'-') :: c :: rest => Character.toUpperCase(c) :: loop(rest)
      case ('_'|'-') :: Nil => Nil
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
  def parameterize(conf: Map[String, Any]): Map[String ,String] = conf map {
    case (k, v) => (camelize(k), v.toString)
  }

  def isNumber(str: String) =
    str.matches(s"""[+-]?((\d+(e\d+)?[lL]?)|(((\d+(\.\d*)?)|(\.\d+))(e\d+)?[fF]?))""")

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

  /**
    * Alternative implementation using a Custom Interpolator.
    *
    * @ param sc StringContext
    *
  *implicit class TemplateHelper(sc: StringContext) extends AnyVal {

    *def t(args: Any*): String = {
      *val strings = sc.parts.iterator
      *val expressions = args.iterator
      *val sb = new StringBuilder(strings.next)
      *while (strings.hasNext) {
        *sb append expressions.next
        *sb append strings.next
      *}
      *sb.toString
    *}

  *}*/

  /**
    * Score DataFrame using POJO model from H2O.
    *
    * If response = true then response must be final column of df and
    * of String type (at this stage only implemented for classification).
    *
    * @author Todd Niven
    * @param model GenModel
    * @param df DataFrame
    * @param responseAttached Boolean
    * @return RDD of Arrays of Doubles
    */
  def score(model: GenModel, df: RDD[Row], responseAttached: Boolean): RDD[Array[Double]] = {
    val domainValues = model.getDomainValues
    val responseMap = if (responseAttached) {
      domainValues.last.view.zipWithIndex.toMap
    } else null
    // convert each Row into an Array of Doubles
    df.map(row => {
      val rRecoded = for (i <- 0 to domainValues.length - 2) yield row(i) match {
        case x: Any if domainValues(i) != null => model.mapEnum(model.getColIdx(model.getNames()(i)), x.toString).toDouble
        case null if domainValues(i) != null => -1.0
        case x: Int => x.toDouble
        case x: Double => x.toDouble
        case _ => 0.0 // default to 0 if null found in numeric column
      }
      // run model on encoded rows
      // if responseAttached = true then output response is the last column as Double
      if (responseAttached) {
        model.score0(rRecoded.toArray, Array(model.getNumResponseClasses + 1.0)) ++ Array(responseMap(row.getString(row.length - 1)).toDouble)
      } else {
        model.score0(rRecoded.toArray, Array(model.getNumResponseClasses + 1.0))
      }
    })
  }

}
