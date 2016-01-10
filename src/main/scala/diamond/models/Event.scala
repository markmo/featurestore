package diamond.models

import java.util.Date

/**
  * @param entity String entity id (usually hashed)
  * @param attribute String attribute name
  * @param ts Date event timestamp
  * @param namespace String logical grouping
  * @param value String value pertinent to the event, e.g. page URL
  * @param properties String JSON document of related information
  * @param source String source system
  * @param processId String id of process that created event record
  * @param processTime Date datetime of record creation
  * @param version Int version number, incremented for changed/fixed record
  *
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

  // order events by natural key (entity, attribute, ts, version)
  def compare(that: Event): Int =
    (entity, attribute, ts, version) compare(that.entity, that.attribute, that.ts, that.version)

}
