package common.utility

import hex.genmodel.GenModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Created by markmo on 27/02/2016.
  */
object scoringFunctions {

  /**
    * Score DataFrame using POJO model from H2O.
    *
    * If response = true then response must be final column of df and
    * of String type (at this stage only implemented for classification).
    *
    * @author Todd Niven
    * @param model GenModel
    * @param df DataFrame
    * @param responseAttached Boolean
    * @return RDD of Arrays of Doubles
    */
  def score(model: GenModel, df: RDD[Row], responseAttached: Boolean): RDD[Array[Double]] = {
    val domainValues = model.getDomainValues
    val responseMap = if (responseAttached) {
      domainValues.last.view.zipWithIndex.toMap
    } else null
    // convert each Row into an Array of Doubles
    df.map(row => {
      val rRecoded = for (i <- 0 to domainValues.length - 2) yield row(i) match {
        case x: Any if domainValues(i) != null => model.mapEnum(model.getColIdx(model.getNames()(i)), x.toString).toDouble
        case null if domainValues(i) != null => -1.0
        case x: Int => x.toDouble
        case x: Double => x.toDouble
        case _ => 0.0 // default to 0 if null found in numeric column
      }
      // run model on encoded rows
      // if responseAttached = true then output response is the last column as Double
      if (responseAttached) {
        model.score0(
          rRecoded.toArray,
          Array(model.getNumResponseClasses + 1.0)) ++ Array(responseMap(row.getString(row.length - 1)).toDouble
        )
      } else {
        model.score0(rRecoded.toArray, Array(model.getNumResponseClasses + 1.0))
      }
    })
  }

}
