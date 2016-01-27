package diamond.utility

import java.math.BigInteger
import java.security.MessageDigest
import java.text.SimpleDateFormat

import hex.genmodel.GenModel
import net.openhft.hashing.LongHashFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.annotation.tailrec

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

      @tailrec
      def loop(str: String): String = {
        if (str.length == 0) {
          sb.toString
        } else if (str.startsWith("$$")) {
          sb.append("$$")
          loop(str.substring(2))
        } else if (str.startsWith("${")) {
          val i = str.indexOf("}")
          if (i == -1) {
            sb.append(str).toString
          } else {
            val replacement = vars.get(str.substring(2, i)).orNull
            if (replacement == null) {
              sb.append("${")
              loop(str.substring(2))
            } else {
              sb.append(replacement)
              loop(str.substring(i + 1))
            }
          }
        } else if (str.startsWith("$")) {
          val n = str.length
          if (n == 1) {
            sb.append(str).toString
          } else {
            val i = str.indexOf(" ")
            val j = if (i == -1) n else i
            val replacement = vars.get(str.substring(1, j)).orNull
            if (replacement == null) {
              sb.append("$")
              loop(str.substring(1))
            } else {
              sb.append(replacement)
              if (j == n) {
                sb.toString
              } else {
                loop(str.substring(j + 1))
              }
            }
          }
        } else {
          val i = str.indexOf("$")
          if (i == -1) {
            sb.append(str).toString
          } else {
            sb.append(str.substring(0, i))
            loop(str.substring(i))
          }
        }
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
