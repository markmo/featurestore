package diamond.models

import java.util.Date

/**
  * Created by markmo on 30/11/2015.
  */
case class Event(entity: String,
                 attribute: String,
                 ts: Date,
                 namespace: String,
                 value: String,
                 properties: String,
                 source: String,
                 processId: String,
                 processTime: Date,
                 version: Int      // trial - processTime could be used instead
                ) extends Ordered[Event] with Serializable {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: Event): Int =
    (entity, attribute, ts, version) compare (that.entity, that.attribute, that.ts, that.version)

}
