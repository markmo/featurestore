package diamond.utility

import java.util.Date

import diamond.models.Event

import scala.math.Ordered.orderingToOrdered

/**
  * Created by markmo on 27/02/2016.
  */

object EventReverseChronologicalOrdering extends Ordering[Event] {

  def compare(a: Event, b: Event) =
    (a.entity, a.ts) compare ((b.entity, b.ts))

}

object EventChronologicalOrdering extends Ordering[Event] {

  def compare(a: Event, b: Event) =
    (b.entity, b.ts) compare ((a.entity, a.ts))

}

object TimeEventOrdering extends Ordering[(Date, Event)] {

  def compare(a: (Date, Event), b: (Date, Event)) = a._1 compare b._1

}

object TimeEventCountOrdering extends Ordering[(Date, (Event, Int))] {

  def compare(a: (Date, (Event, Int)), b: (Date, (Event, Int))) = a._1 compare b._1

}

object EventDateOrdering extends Ordering[(String, (Event, Date, Date))] {

  def compare(a: (String, (Event, Date, Date)), b: (String, (Event, Date, Date))) =
    if (a._1 == b._1) {
      b._2._3 compare a._2._3
    } else {
      a._1 compare b._1
    }

}
