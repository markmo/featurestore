package diamond.models

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by markmo on 12/01/2016.
  */
case class JobStep(stepName: String, status: String, ts: Date, message: Option[String] = None) {

  def toArray = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    Array(stepName, status, dateFormatter.format(ts), message.getOrElse(""))
  }

}
