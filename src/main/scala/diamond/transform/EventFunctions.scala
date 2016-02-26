package diamond.transform

import java.util.{Calendar, Date}

import diamond.models.Event
import diamond.utility.{BoundedPriorityQueue, EventChronologicalOrdering, EventReverseChronologicalOrdering, TimeEventOrdering}
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Days}

import scala.collection.mutable

/**
  * Created by markmo on 20/01/2016.
  */
object EventFunctions {

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

    def previousInteractions(n: Int, asof: Date): Map[String, Iterable[Event]] = {
      type Heap = BoundedPriorityQueue[(Date, Event)]
      val dates: RDD[((String, Event), Date)] = events.current()
        .filter(_.ts <= asof)
        .map(ev => (ev.entity, ev) -> ev.ts)
      val perPartition: RDD[Map[String, Heap]] = dates.mapPartitions { xs =>
        val heaps = mutable.Map[String, Heap]().withDefault(_ => new Heap(n)(TimeEventOrdering))
        for (((entity, ev), ts) <- xs) {
          heaps(entity) += ts -> ev
        }
        Iterator.single(heaps.toMap)
      }
      val merged: Map[String, Heap] = perPartition.reduce { (hs1, hs2) =>
        val heaps = mutable.Map[String, Heap]().withDefault(_ => new Heap(n)(TimeEventOrdering))
        for ((entity, heap) <- hs1.toSeq ++ hs2.toSeq) {
          for (x <- heap) {
            heaps(entity) += x
          }
        }
        heaps.toMap
      }
      merged.mapValues(_.map { case (ts, ev) => ev })
    }

    def previousInteractions(eventType: String, n: Int, asof: Date): Map[String, Iterable[Event]] = {
      type Heap = BoundedPriorityQueue[(Date, Event)]
      val cal = Calendar.getInstance()
      cal.set(1970, 0, 1)
      val chordTimes = events.chord(eventType).map {
        case (entity, Some(ev)) => (entity, ev.ts)
        case (entity, None) => (entity, cal.getTime)
      }
      chordTimes.cache()
      val dates: RDD[((String, Event), (Date, Date))] = events.current()
        .filter(_.ts <= asof)
        .map(ev => (ev.entity, ev))
        .join(chordTimes)
        .map {
          case (entity, (ev, chordTime)) => ((entity, ev), (chordTime, ev.ts))
        }

      val perPartition: RDD[Map[String, Heap]] = dates.mapPartitions { xs =>
        val heaps = mutable.Map[String, Heap]().withDefault(_ => new Heap(n)(TimeEventOrdering))
        for (((entity, ev), (chordTime, ts)) <- xs if ts <= chordTime) {
          heaps(entity) += ts -> ev
        }
        Iterator.single(heaps.toMap)
      }

      val merged: Map[String, Heap] = perPartition.reduce { (hs1, hs2) =>
        val heaps = mutable.Map[String, Heap]().withDefault(_ => new Heap(n)(TimeEventOrdering))
        for ((entity, heap) <- hs1.toSeq ++ hs2.toSeq) {
          for (x <- heap) {
            heaps(entity) += x
          }
        }
        heaps.toMap
      }

      val sc = events.sparkContext
      val evMap = merged.mapValues(_.map { case (ts, ev) => ev })
      val evs = sc.parallelize(evMap.toSeq)

      val result = chordTimes.leftOuterJoin(evs).map {
        case (entity, (_, Some(es))) => (entity, es)
        case (entity, (_, None)) => (entity, Nil)
      }.collectAsMap()
      chordTimes.unpersist()
      result.toMap
    }

    def current(): RDD[Event] = {
      events.map(ev => (ev.entity, ev))
        .reduceByKey((a, b) => if (a.version > b.version) a else b)
        .map(_._2)
    }

    def chord(eventType: String): RDD[(String, Option[Event])] = {
      val entities = events.map(_.entity).distinct().map(_ -> None)
      val evs = events.filter(_.eventType == eventType)
        .map(ev => (ev.entity, ev))
        .reduceByKey((a, b) => if (a.ts.after(b.ts)) a else b)

      entities.leftOuterJoin(evs).map {
        case (entity, (_, maybeEvent)) => (entity, maybeEvent)
      }
    }

  }

  def paths(evMap: Map[String, Iterable[Event]]): Map[String, String] =
    evMap.mapValues(_.map(_.eventType).mkString(","))

}
