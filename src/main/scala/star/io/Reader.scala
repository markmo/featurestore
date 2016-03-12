package star.io

import org.apache.spark.sql.{DataFrame, SQLContext}
import star.StarConfig

/**
  * Created by markmo on 12/03/2016.
  */
trait Reader {

  val sqlContext: SQLContext

  val conf: StarConfig

  def read(source: String): DataFrame

}
