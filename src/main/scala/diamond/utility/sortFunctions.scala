package diamond.utility

import scala.annotation.tailrec

/**
  * Created by markmo on 12/12/2015.
  */
object sortFunctions {

  /**
    * Topological sort. Used to sort the Pipeline DAG to perform
    * independent transformations before dependent transformations.
    *
    * @param edges Traversable[(A, A)]
    * @tparam A
    * @return an Iterable of A
    */
  def tsort[A](edges: Traversable[(A, A)]): Iterable[A] = {
    @tailrec
    def tsort(toPreds: Map[A, Set[A]], done: Iterable[A]): Iterable[A] = {
      val (noPreds, hasPreds) = toPreds.partition(_._2.isEmpty)
      if (noPreds.isEmpty) {
        if (hasPreds.isEmpty) done else sys.error(hasPreds.toString)
      } else {
        val found = noPreds.keys
        tsort(hasPreds.mapValues { _ -- found }, done ++ found)
      }
    }

    val toPred = edges.foldLeft(Map[A, Set[A]]()) { (acc, e) =>
      acc + (e._1 -> acc.getOrElse(e._1, Set())) + (e._2 -> (acc.getOrElse(e._2, Set()) + e._1))
    }

    tsort(toPred, Seq())
  }

}
