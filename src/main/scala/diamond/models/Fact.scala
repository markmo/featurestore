package diamond.models

import java.util.Date

/**
  * @param entity String entity id (usually hashed)
  * @param attribute String attribute name
  * @param ts Date fact state change timestamp
  * @param namespace String logical grouping
  * @param value String value pertinent to the fact, e.g. 9
  * @param properties String JSON document of related information
  * @param startTime Date
  * @param endTime Date
  * @param source String source system
  * @param processType String
  * @param processId String id of process that created fact record
  * @param processDate Date datetime of record creation
  * @param userId String
  * @param rectype String
  * @param version Int version number, incremented for changed/fixed record
  *
  * Created by markmo on 30/11/2015.
  */
case class Fact(entity: String,
                attribute: String,
                ts: Date,
                namespace: String,
                value: String,
                properties: String,
                startTime: Date,
                endTime: Date,
                source: String,
                processType: String,
                processId: String,
                processDate: Date,
                userId: String,
                rectype: String,
                version: Int // trial - processTime could be used instead
               ) extends Serializable
//               ) extends Ordered[Fact] with Serializable {

//  import scala.math.Ordered.orderingToOrdered

  // order facts by natural key (entity, attribute, ts, version)
//  def compare(that: Fact): Int =
//    (that.entity, that.attribute, that.ts, that.version) compare ((this.entity, this.attribute, this.ts, this.version))

//}
