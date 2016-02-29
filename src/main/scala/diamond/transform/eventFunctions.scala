package diamond.transform

import java.util.{Calendar, Date}

import diamond.models.Event
import diamond.utility._
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.joda.time.{DateTime, Days}

import scala.collection.mutable

/**
  * Created by markmo on 20/01/2016.
  */
object eventFunctions {

  /**
    * Using the "pimp my library" pattern.
    *
    * @param events : RDD[Event]
    */
  implicit class RichEvents(val events: RDD[Event]) extends AnyVal {

    import diamond.utility.dateFunctions._

    def count(attribute: String, startTime: Date, endTime: Date): Long =
      events.filter(ev => ev.eventType == attribute && ev.ts >= startTime && ev.ts <= endTime)
        .count()

    // TODO
    // count within interval

    def countUnique(attribute: String, startTime: Date, endTime: Date): Long =
      events.filter(ev => ev.eventType == attribute && ev.ts >= startTime && ev.ts <= endTime)
        .map(_.value)
        .distinct()
        .count()

    // TODO
    // countUnique within interval

    def sum(attribute: String, startTime: Date, endTime: Date): Double =
      events.filter(ev => ev.eventType == attribute && ev.ts >= startTime && ev.ts <= endTime)
        .map(_.value.toDouble)
        .sum()

    // TODO
    // sum within interval

    def daysSinceLatest(attribute: String, date: Date = new Date): Int = {
      val latest = events.filter(ev => ev.eventType == attribute && ev.ts <= date)
        .takeOrdered(1)(EventReverseChronologicalOrdering)(0)
      Days.daysBetween(new DateTime(latest.ts), new DateTime(date)).getDays
    }

    def daysSinceEarliest(attribute: String, date: Date = new Date): Int = {
      val earliest = events.filter(ev => ev.eventType == attribute && ev.ts <= date)
        .takeOrdered(1)(EventChronologicalOrdering)(0)
      Days.daysBetween(new DateTime(earliest.ts), new DateTime(date)).getDays
    }

    /**
      * Sessionize a sequence of events based on grouping events separated by
      * less than the timeout.
      *
      * @param timeout Duration in milliseconds
      * @return RDD[Event] updated sessionized events
      */
    def sessionize(timeout: Duration): RDD[Event] = {
      if (events.count < 2) {
        events
      } else {
        events.current().map(ev => (ev.entity, ev)).groupByKey().flatMap {
          case (entity, ev) =>
            val sorted = ev.toSeq.sorted(EventReverseChronologicalOrdering)
            val head = sorted.head.copy(session = 1)
            sorted.tail.scanLeft(head) { case (prev, cur) =>
              // calculate session
              def session = if (sessionTimedOut) prev.session + 1 else prev.session

              // returns true if the session has timed out between the prev and cur event
              def sessionTimedOut = cur.ts.getTime - prev.ts.getTime > timeout.milliseconds

              cur.copy(session = session)
            }
        }
      }
    }

    /**
      * Extract a list of event paths for each entity.
      *
      * @param n Int max path size
      * @param asof Date include only interactions before or on this date
      * @return RDD[(String, Array[Event])] event paths keyed by entity
      */
    def previousInteractions(n: Int, asof: Date): RDD[(String, Array[Event])] = {
      events
        .current() // include only current versions of events
        .filter(_.ts <= asof) // include only events before or on the asof date
        .map(ev => (ev.entity, ev))
        .topByKey(n)(EventReverseChronologicalOrdering)
    }

    /**
      * Extract the event path to a specified size for each entity. Each path
      * ends at the event with the given eventType, which means that each path
      * ends at a different point in time for each entity. This arrangement is
      * called a chord because the same type of event is lined up across
      * entities like fingers forming a chord on a guitar. For example, a
      * purchase will occur at a different time for each customer. It is
      * common to train a model based on the history before or after a
      * significant event.
      *
      * @param eventType String
      * @param n Int max path size
      * @param asof Date include only interactions before or on this date
      * @return Map[String, Iterable[Event event paths keyed by entity
      */
    def previousInteractions(eventType: String, n: Int, asof: Date): Map[String, Iterable[Event]] = {
      type Heap = BoundedPriorityQueue[(Date, Event)]
      val cal = Calendar.getInstance

      // min default date
      cal.set(1970, 0, 1)

      val chords = events.extractChords(eventType).map {
        case (entity, Some(ev)) => (entity, ev.ts)
        case (entity, None) => (entity, cal.getTime)
      }
      chords.cache()

      // RDD of (entity, (Event, chord, event.ts))
      val times: RDD[(String, (Event, Date, Date))] = events
        .current() // include only current versions of events
        .filter(_.ts <= asof) // include only events before or on the asof date
        .map(ev => (ev.entity, ev))
        .join(chords) // add in chord time for entity
        .map { case (entity, (ev, chord)) => (entity, (ev, chord, ev.ts)) }

      val perPartition: RDD[Map[String, Heap]] = times.mapPartitions { xs =>
        val heaps = mutable.Map[String, Heap]()
        for ((entity, (ev, chord, ts)) <- xs.toSeq.sorted(EventDateOrdering) if ts <= chord) {
          val heap = if (heaps.contains(entity)) heaps(entity) else new Heap(n)(TimeEventOrdering)
          heap += ((ts, ev))
          heaps(entity) = heap
        }
        // trick to create an RDD
        Iterator.single(heaps.toMap)
      }

      val merged: Map[String, Heap] = perPartition.reduce { (a, b) =>
        val heaps = mutable.Map[String, Heap]()
        for ((entity, heap) <- (a.toSeq ++ b.toSeq).sortBy(_._1)) {
          for (x <- heap.sorted) {
            val heap = if (heaps.contains(entity)) heaps(entity) else new Heap(n)(TimeEventOrdering)
            heap += x
            heaps(entity) = heap
          }
        }
        heaps.toMap
      }

      val sc = events.sparkContext
      val byEntity = merged.mapValues(_.map(_._2))
      val byEntityRDD = sc.parallelize(byEntity.toSeq)

      // include all entities in result even if nil events
      val result = chords.leftOuterJoin(byEntityRDD).map {
        case (entity, (_, Some(es))) => (entity, es)
        case (entity, (_, None)) => (entity, Nil)
      }.collectAsMap()

      chords.unpersist()

      result.toMap
    }

    /**
      * Similar to `previousInteractions` except paths will collapse contiguous
      * events of the same type, including an occurrence count with each event
      * in the path.
      *
      * @param eventType String
      * @param n Int max path size
      * @param asof Date include only interactions before or on this date
      * @return Map[String, Iterable[(Event, Int) event paths keyed by entity
      */
    def previousUniqueInteractions(eventType: String, n: Int, asof: Date): Map[String, Iterable[(Event, Int)]] = {
      type Heap = BoundedPriorityQueue[(Date, (Event, Int))]
      val cal = Calendar.getInstance

      // min default date
      cal.set(1970, 0, 1)

      val chords = events.extractChords(eventType).map {
        case (entity, Some(ev)) => (entity, ev.ts)
        case (entity, None) => (entity, cal.getTime)
      }
      chords.cache()

      // RDD of (entity, (Event, chord, event.ts))
      val times: RDD[(String, (Event, Date, Date))] = events
        .current()
        .filter(_.ts <= asof)
        .map(ev => (ev.entity, ev))
        .join(chords)
        .map {
          case (entity, (ev, chord)) => (entity, (ev, chord, ev.ts))
        }

      val perPartition: RDD[Map[String, Heap]] = times.mapPartitions { xs =>
        val heaps = mutable.Map[String, Heap]()
        // current tuple of event and count
        var cur: (Event, Int) = (null, 0)
        for ((entity, (ev, chord, ts)) <- xs.toSeq.sorted(EventDateOrdering) if ts <= chord) {
          val (e, k) = cur
          if (e == null) {
            cur = (ev, 1)
          } else if (entity == e.entity && ev.eventType == e.eventType) {
            // if same entity and event type then increment count
            // keep latest event
            if (ts after e.ts) {
              cur = (ev, k + 1)
            } else {
              cur = (e, k + 1)
            }
          } else {
            // method doesn't work if new Heap set as default value on heaps map
            val heap = if (heaps.contains(e.entity)) heaps(e.entity) else new Heap(n)(TimeEventCountOrdering)
            heap += ((e.ts, (e, k)))
            heaps(e.entity) = heap
            cur = (ev, 1)
          }
        }
        val (e, _) = cur
        val heap = if (heaps.contains(e.entity)) heaps(e.entity) else new Heap(n)(TimeEventCountOrdering)
        heap += ((e.ts, cur))
        heaps(e.entity) = heap

        // trick to create an RDD
        Iterator.single(heaps.toMap)
      }

      val merged: Map[String, Heap] = perPartition.reduce { (a, b) =>
        val heaps = mutable.Map[String, Heap]()
        var cur: (Event, Int) = (null, 0)
        for ((entity, heap) <- (a.toSeq ++ b.toSeq).sortBy(_._1)) {
          for ((ts, (ev, k)) <- heap.sorted) {
            val (e, k) = cur
            if (e == null) {
              cur = (ev, 1)
            } else if (entity == e.entity && ev.eventType == e.eventType) {
              if (ts after e.ts) {
                cur = (ev, k + 1)
              } else {
                cur = (e, k + 1)
              }
            } else {
              val heap = if (heaps.contains(e.entity)) heaps(e.entity) else new Heap(n)(TimeEventCountOrdering)
              heap += ((e.ts, (e, k)))
              heaps(e.entity) = heap
              cur = (ev, 1)
            }
          }
        }
        val (e, _) = cur
        val heap = if (heaps.contains(e.entity)) heaps(e.entity) else new Heap(n)(TimeEventCountOrdering)
        heap += ((e.ts, cur))
        heaps(e.entity) = heap
        heaps.toMap
      }

      val sc = events.sparkContext
      val byEntity = merged.mapValues(_.map(_._2))
      val byEntityRDD = sc.parallelize(byEntity.toSeq)

      // include all entities in result even if nil events
      val result = chords.leftOuterJoin(byEntityRDD).map {
        case (entity, (_, Some(es))) => (entity, es)
        case (entity, (_, None)) => (entity, Nil)
      }.collectAsMap()

      chords.unpersist()

      result.toMap
    }

    /**
      * Return the current (latest) version of each event. Typically, an event
      * will have multiple versions if the record required updating due to
      * some error.
      *
      * @return RDD[Event]
      */
    def current(): RDD[Event] = {
      events.map(ev => ((ev.entity, ev.eventType, ev.ts), ev))
        .reduceByKey((a, b) => if (a.version > b.version) a else b)
        .map(_._2)
    }

    /**
      * Extract chords, ie. the previous event of the specified type for each
      * entity. This arrangement is called a chord because the same type of
      * event is lined up across entities like fingers forming a chord on a
      * guitar.
      *
      * @param eventType String
      * @return RDD[(String, Option[Event])]
      */
    def extractChords(eventType: String): RDD[(String, Option[Event])] = {
      val entities = events.map(_.entity).distinct().map(_ -> None)
      val evs = events.filter(_.eventType == eventType)
        .map(ev => (ev.entity, ev))
        .reduceByKey((a, b) => if (a.ts.after(b.ts)) a else b)

      entities.leftOuterJoin(evs).map {
        case (entity, (_, maybeEvent)) => (entity, maybeEvent)
      }
    }

  }

  /**
    * Convert event paths as lists to comma separated strings.
    *
    * @param eventsByEntity Map[String, Iterable[Event
    * @return Map[String, String]
    */
  def paths(eventsByEntity: Map[String, Iterable[Event]]): Map[String, String] =
    eventsByEntity.mapValues(_.map(_.eventType).mkString(","))

  /**
    * Convert paths of unique events, that contain an occurrence count, to
    * comma separated strings.
    *
    * @param eventsByEntity Map[String, Iterable[(Event, Int)
    * @return Map[String, String]
    */
  def uniquePaths(eventsByEntity: Map[String, Iterable[(Event, Int)]]): Map[String, String] =
    eventsByEntity.mapValues(_.map(_._1.eventType).mkString(","))

}
