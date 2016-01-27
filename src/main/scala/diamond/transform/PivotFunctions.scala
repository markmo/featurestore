package diamond.transform

import java.util.Date

import diamond.models.Event
import diamond.store.FeatureStore
import org.apache.spark.rdd.RDD

/**
  * Created by markmo on 12/12/2015.
  */
object PivotFunctions {

  /**
    * pivot function - snapshot
    *     extract the latest values for attributes across entities
    *     with respect to a given point in time
    *
    * At a high-level, chord extractions are typically performed when preparing feature vectors for model training.
    * Snapshot extractions are typically performed when preparing feature vectors for model scoring.
    */
  def snapshot(events: RDD[Event], dt: Date, store: FeatureStore): RDD[Seq[Any]] =
    events
      .filter(ev => ev.ts.before(dt) || ev.ts.equals(dt))
      .map(ev => (ev.entity, ev))
      .groupByKey
      .map {
        case (key, evs) =>
          val latestEvents =
            evs
              // group all events for entity by type
              // returns Map(type -> List((type, event)))
              .map(ev => (ev.attribute, ev))
              .groupBy(_._1)

              // return the event in the first tuple
              // sorted by latest first
              .map {
                case (_, es) => es.map(_._2).toList.sorted.head
              }

          // create a map of the features that an entity has
          val featureMap = latestEvents.map(ev => (ev.attribute, ev.value)).toMap

          // iterate through registered events finding a matching value
          // or null if not found
          val features = store.registeredFeatures.map(ev => featureMap.getOrElse(ev.attribute, null))

          // merge the key tuple to return a flat list
          List(key) ++ features
      }

  /**
    * pivot function - chord
    *     extract the latest values for attributes across entities
    *     with respect to given points in time for each entity
    */
  def chord(events: RDD[Event], attribute: String, dt: Date, store: FeatureStore) =
    events
      .filter(ev => ev.ts.before(dt) || ev.ts.equals(dt))
      .map(ev => (ev.entity, ev))
      .groupByKey
      .map {
        case (key, evs) =>
          // group all events for entity by type
          // returns Map(type -> List((type, event)))
          val grouped = evs.map(ev => (ev.attribute, ev)).groupBy(_._1)
          if (grouped.contains(attribute)) {
            val syncTime = grouped(attribute).map(_._2).toList.sorted.head.ts
            val chordEvents =
              grouped
                .map {
                  case (_, es) =>
                    es
                      .map(_._2)
                      .filter(ev => ev.ts.before(syncTime) || ev.ts.equals(syncTime))
                      .toList.sorted.head
                }

            // create a map of the features that an entity has
            val featureMap = chordEvents.map(ev => (ev.attribute, ev.value)).toMap

            // iterate through registered events finding a matching value
            // or null if not found
            val features = store.registeredFeatures.map(ev => featureMap.getOrElse(ev.attribute, null))

            // merge the key tuple to return a flat list
            List(syncTime) ++ List(key) ++ features
          } else {
            Nil
          }
      }

}
