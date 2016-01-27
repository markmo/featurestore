package diamond.transform

import java.util.Date

import diamond.models.Event
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Days}

import scala.math.Ordered.orderingToOrdered

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

    def count(attribute: String, startTime: Date, endTime: Date): Long =
      events.filter(ev =>
        ev.attribute == attribute &&
          (ev.ts.after(startTime) || ev.ts.equals(startTime)) &&
          (ev.ts.before(endTime) || ev.ts.equals(endTime))
      ).count()

    // TODO
    // count within interval

    def countUnique(attribute: String, startTime: Date, endTime: Date): Long =
      events.filter(ev =>
        ev.attribute == attribute &&
          (ev.ts.after(startTime) || ev.ts.equals(startTime)) &&
          (ev.ts.before(endTime) || ev.ts.equals(endTime))
      ).map(_.value).distinct().count()

    // TODO
    // countUnique within interval

    def sum(attribute: String, startTime: Date, endTime: Date): Double =
      events.filter(ev =>
        ev.attribute == attribute &&
          (ev.ts.after(startTime) || ev.ts.equals(startTime)) &&
          (ev.ts.before(endTime) || ev.ts.equals(endTime))
      ).map(_.value.toDouble).sum()

    // TODO
    // sum within interval

    def daysSinceLatest(attribute: String, date: Date = new Date): Int = {
      val latest = events.filter(ev =>
        ev.attribute == attribute && (ev.ts.before(date) || ev.ts.equals(date))
      ).takeOrdered(1)(EventReverseChronologicalOrdering)(0)
      Days.daysBetween(new DateTime(latest.ts), new DateTime(date)).getDays
    }

    def daysSinceEarliest(attribute: String, date: Date = new Date): Int = {
      val earliest = events.filter(ev =>
        ev.attribute == attribute && (ev.ts.before(date) || ev.ts.equals(date))
      ).takeOrdered(1)(EventChronologicalOrdering)(0)
      Days.daysBetween(new DateTime(earliest.ts), new DateTime(date)).getDays
    }

  }

}

object EventReverseChronologicalOrdering extends Ordering[Event] {

  def compare(e1: Event, e2: Event) =
    -(e1.entity, e1.attribute, e1.ts, e1.version).compare((e2.entity, e2.attribute, e2.ts, e2.version))

}

object EventChronologicalOrdering extends Ordering[Event] {

  def compare(e1: Event, e2: Event) =
    (e1.entity, e1.attribute, e1.ts, e1.version).compare((e2.entity, e2.attribute, e2.ts, e2.version))

}
