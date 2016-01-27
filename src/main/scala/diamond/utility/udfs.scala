package diamond.utility

import functions._
import org.apache.spark.sql.functions._

/**
  * Created by markmo on 12/12/2015.
  */
object udfs {

  def convertStringToDateUDF = udf(convertStringToDate(_: String, _: String))

  def formatDateStringUDF = udf(formatDateString(_: String, _: String))

  def formatDateTimeStringUDF = udf(formatDateTimeString(_: String, _: String))

  def convertStringToTimestampUDF = udf(convertStringToTimestamp(_: String, _: String))

  def hashKeyUDF = udf(hashKey(_: String))

  def fastHashUDF = udf(fastHash(_: String))

}
