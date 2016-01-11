package diamond.models

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row

/**
  * Created by markmo on 11/01/2016.
  */
case class TransformationError(stepName: String, ts: Date, errorType: String, message: String, row: Row) {

  def toArray = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    Array(stepName, dateFormatter.format(ts), errorType, message) ++ row.toSeq
  }

}

class ErrorThresholdReachedException(stepName: String, ts: Date)
  extends Exception(s"Error threshold reached on step $stepName")
