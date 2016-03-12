package diamond.models

import java.util.Date

/**
  * @param entity String entity id (usually hashed)
  * @param eventType String attribute name
  * @param ts Date event timestamp
  * @param namespace String logical grouping
  * @param session Option[String] session id
  * @param task Option[Task]
  * @param value String value pertinent to the event, e.g. page URL
  * @param properties String JSON document of related information
  * @param startTime Date
  * @param endTime Date
  * @param source String source system
  * @param processType String
  * @param processId String id of process that created event record
  * @param processDate Date datetime of record creation
  * @param userId String
  * @param rectype String
  * @param version Int version number, incremented for changed/fixed record
  *
  * Created by markmo on 30/11/2015.
  */
case class Event(entity: String,
                 eventType: String,
                 ts: Date,
                 namespace: String = "default",
                 session: Int,
                 task: Option[Task],
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
                 version: Int
                ) extends Serializable
