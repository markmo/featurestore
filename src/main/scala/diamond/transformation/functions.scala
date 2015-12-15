package diamond.transformation

import java.math.BigInteger
import java.security.MessageDigest
import java.text.SimpleDateFormat

/**
  * Created by markmo on 30/11/2015.
  */
object functions {

  val OUTPUT_DATE_PATTERN = "yyyy-MM-dd"

  val OUTPUT_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss"

  def convertStringToDate(str: String, pattern: String) = {
    val format = new SimpleDateFormat(pattern)
    format.parse(str)
  }

  def formatDateString(str: String, pattern: String) = {
    val dt = convertStringToDate(str, pattern)
    val outputFormat = new SimpleDateFormat(OUTPUT_DATE_PATTERN)
    outputFormat.format(dt)
  }

  def formatDateTimeString(str: String, pattern: String) = {
    val dt = convertStringToDate(str, pattern)
    val outputFormat = new SimpleDateFormat(OUTPUT_DATE_TIME_PATTERN)
    outputFormat.format(dt)
  }

  def convertStringToTimestamp(str: String, pattern: String) = {
    convertStringToDate(str, pattern).getTime
  }

  def hashKey(key: String) = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(key.getBytes("UTF-8"))
    val digest = md.digest()
    String.format("%064x", new BigInteger(1, digest))
  }

}
