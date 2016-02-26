package diamond.utility

import java.util.Date

import diamond.models.Event

import scala.math.Ordered.orderingToOrdered

/**
  * Created by markmo on 27/02/2016.
  */

object EventReverseChronologicalOrdering extends Ordering[Event] {

  def compare(e1: Event, e2: Event) =
    -(e1.entity, e1.eventType, e1.ts, e1.version).compare((e2.entity, e2.eventType, e2.ts, e2.version))

}

object EventChronologicalOrdering extends Ordering[Event] {

  def compare(e1: Event, e2: Event) =
    (e1.entity, e1.eventType, e1.ts, e1.version).compare((e2.entity, e2.eventType, e2.ts, e2.version))

}

object TimeEventOrdering extends Ordering[(Date, Event)] {

  def compare(e1: (Date, Event), e2: (Date, Event)) = e1._1 compare e2._1

}